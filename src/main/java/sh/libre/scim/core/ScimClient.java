package sh.libre.scim.core;

import java.util.HashMap;
import java.util.Map;

import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.ws.rs.ProcessingException;

import de.captaingoldfish.scim.sdk.client.ScimClientConfig;
import de.captaingoldfish.scim.sdk.client.ScimRequestBuilder;
import de.captaingoldfish.scim.sdk.client.http.BasicAuth;
import de.captaingoldfish.scim.sdk.client.response.ServerResponse;
import de.captaingoldfish.scim.sdk.common.exceptions.ResponseException;
import de.captaingoldfish.scim.sdk.common.resources.ResourceNode;
import de.captaingoldfish.scim.sdk.common.response.ListResponse;

import org.jboss.logging.Logger;
import org.keycloak.component.ComponentModel;
import org.keycloak.connections.jpa.JpaConnectionProvider;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RoleMapperModel;
import org.keycloak.storage.user.SynchronizationResult;
import sh.libre.scim.storage.ScimSynchronizationResult;

import com.google.common.net.HttpHeaders;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;


public class ScimClient {
    final protected Logger LOGGER = Logger.getLogger(ScimClient.class);
    final protected ScimRequestBuilder scimRequestBuilder;
    final protected RetryRegistry registry;
    final protected KeycloakSession session;
    final protected String contentType;
    final protected ComponentModel model;
    final protected String scimApplicationBaseUrl;
    final protected Map<String, String> defaultHeaders;
    final protected Map<String, String> expectedResponseHeaders;

    public ScimClient(ComponentModel model, KeycloakSession session) {
        this.model = model;
        this.contentType = model.get("content-type");
        this.session = session;
        this.scimApplicationBaseUrl = model.get("endpoint");
        this.defaultHeaders = new HashMap<>();
        this.expectedResponseHeaders = new HashMap<>();

        switch (model.get("auth-mode")) {
            case "BEARER":
                defaultHeaders.put(HttpHeaders.AUTHORIZATION,
                    BearerAuthentication(model.get("auth-pass")));
                break;
            case "BASIC_AUTH":
                defaultHeaders.put(HttpHeaders.AUTHORIZATION,
                    BasicAuthentication(model.get("auth-user"),
                                        model.get("auth-pass")));
                break;
        }

        defaultHeaders.put(HttpHeaders.CONTENT_TYPE, contentType);

        scimRequestBuilder = new ScimRequestBuilder(scimApplicationBaseUrl, genScimClientConfig());

        RetryConfig retryConfig = RetryConfig.custom()
            .maxAttempts(10)
            .intervalFunction(IntervalFunction.ofExponentialBackoff())
            .retryExceptions(ProcessingException.class)
            .build();

        registry = RetryRegistry.of(retryConfig);
    }

    protected String BasicAuthentication(String username, String password) {
        return  BasicAuth.builder()
        .username(model.get(username))
        .password(model.get(password))
        .build()
        .getAuthorizationHeaderValue();
    }

    protected ScimClientConfig genScimClientConfig() {
        return ScimClientConfig.builder()
        .httpHeaders(defaultHeaders)
        .connectTimeout(5)
        .requestTimeout(5)
        .socketTimeout(5)
        .expectedHttpResponseHeaders(expectedResponseHeaders)
        .hostnameVerifier((s, sslSession) -> true)
        .build();
    }

    protected String BearerAuthentication(String token) {
        return "Bearer " + token ;
    }

    protected String genScimUrl(String scimEndpoint, String resourcePath) {
        return "%s/%s/%s".formatted(scimApplicationBaseUrl,
                scimEndpoint,
                resourcePath);
    }


    protected EntityManager getEM() {
        return session.getProvider(JpaConnectionProvider.class).getEntityManager();
    }

    protected String getRealmId() {
        return session.getContext().getRealm().getId();
    }

    protected <M extends RoleMapperModel, S extends ResourceNode, A extends Adapter<M, S>> A getAdapter(
            Class<A> aClass) {
        try {
            return aClass.getDeclaredConstructor(KeycloakSession.class, String.class)
                    .newInstance(session, this.model.getId());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <M extends RoleMapperModel, S extends ResourceNode, A extends Adapter<M, S>> void create(Class<A> aClass,
            M kcModel) {
        var adapter = getAdapter(aClass);
        adapter.apply(kcModel);
        if (adapter.skip) {
            return;
        }
        // If mapping exist then it was created by import so skip.
        if (adapter.query("findById", adapter.getId()).getResultList().size() != 0) {
            return;
        }
        LOGGER.debugf("Creating SCIM resource for %s", adapter.getId());
        var retry = registry.retry("create-" + adapter.getId());

        ServerResponse<S> response = retry.executeSupplier(() -> {
            try {
                return scimRequestBuilder
                .create(adapter.getResourceClass(), ("/" + adapter.getSCIMEndpoint()).formatted())
                .setResource(adapter.toSCIM(false))
                .sendRequest();
            } catch (ResponseException e) {
                throw new RuntimeException(e);
            }
        });

        if (!response.isSuccess()){
            LOGGER.warn(response.getResponseBody());
            LOGGER.warn(response.getHttpStatus());
        }

        adapter.apply(response.getResource());
        adapter.saveMapping();
    };

    public <M extends RoleMapperModel, S extends ResourceNode, A extends Adapter<M, S>> void replace(Class<A> aClass,
            M kcModel) {
        var adapter = getAdapter(aClass);
        try {
            adapter.apply(kcModel);
            if (adapter.skip) {
                return;
            }
            var resource = adapter.query("findById", adapter.getId()).getSingleResult();
            adapter.apply(resource);
            String url = genScimUrl(adapter.getSCIMEndpoint(), adapter.getExternalId());
            LOGGER.debugf("Replacing SCIM resource for %s at %s", adapter.getId(), url);
            var retry = registry.retry("replace-" + adapter.getId());
            ServerResponse<S> response = retry.executeSupplier(() -> {
                try {
                    LOGGER.info(adapter.getType());
                    if ((adapter.getType() == "Group" && this.model.get("group-patchOp", false))
                         || (adapter.getType() == "User" && this.model.get("user-patchOp", false))) {
                        return adapter.toPatchBuilder(scimRequestBuilder, url)
                                      .sendRequest();
                    }
                    else {
                        return scimRequestBuilder
                            .update(url, adapter.getResourceClass())
                            .setResource(adapter.toSCIM(false))
                            .sendRequest();
                    }
                } catch (ResponseException e) {
                    throw new RuntimeException(e);
                }
            });
            
            // Handle error responses
            if (!response.isSuccess()) {
                int statusCode = response.getHttpStatus();
                if (statusCode == 405 && adapter.getType().equals("Group") && !this.model.get("group-patchOp", false)) {
                    // PUT not supported for groups, try multiple PATCH operations for Databricks compatibility
                    LOGGER.infof("PUT not supported for groups (405), trying separate PATCH operations for %s", adapter.getId());
                    
                    // For now, just patch members since that's the main issue
                    // TODO: Add support for patching displayName and externalId separately
                    response = adapter.toPatchBuilder(scimRequestBuilder, url).sendRequest();
                } else if (statusCode == 404 || statusCode == 400) {
                    // Resource doesn't exist, create it
                    LOGGER.infof("Resource %s not found (%d), creating instead", adapter.getId(), statusCode);
                    ServerResponse<S> createResponse = scimRequestBuilder
                        .create(adapter.getResourceClass(), ("/" + adapter.getSCIMEndpoint()).formatted())
                        .setResource(adapter.toSCIM(false))
                        .sendRequest();
                    if (createResponse.isSuccess()) {
                        // Update the existing mapping with the new externalId
                        adapter.apply(createResponse.getResource());
                        var existingMapping = adapter.getMapping();
                        if (existingMapping != null) {
                            existingMapping.setExternalId(adapter.getExternalId());
                            getEM().merge(existingMapping);
                        } else {
                            adapter.saveMapping();
                        }
                        response = createResponse; // Use the successful create response
                    } else {
                        response = createResponse; // Return the failed create response for logging
                    }
                }
            }
            
            if (!response.isSuccess()){
                LOGGER.warn(response.getResponseBody());
                LOGGER.warn(response.getHttpStatus());
            }
        } catch (NoResultException e) {
            LOGGER.warnf("failed to replace resource %s, scim mapping not found", adapter.getId());
        } catch (Exception e) {
            LOGGER.error(e);
        }
    }

    public <M extends RoleMapperModel, S extends ResourceNode, A extends Adapter<M, S>> void delete(Class<A> aClass,
            String id) {
        var adapter = getAdapter(aClass);
        adapter.setId(id);
        LOGGER.debugf("Deleting SCIM resource for %s", id);

        try {
            var resource = adapter.query("findById", adapter.getId()).getSingleResult();
            adapter.apply(resource);

            var retry = registry.retry("delete-" + id);

            ServerResponse<S> response = retry.executeSupplier(() -> {
                try {
                    return scimRequestBuilder.delete(genScimUrl(adapter.getSCIMEndpoint(), adapter.getExternalId()),
                                                                adapter.getResourceClass())
                                             .sendRequest();
                } catch (ResponseException e) {
                    throw new RuntimeException(e);
                }
            });

            if (!response.isSuccess()){
                LOGGER.warn(response.getResponseBody());
                LOGGER.warn(response.getHttpStatus());
            }

            getEM().remove(resource);

        } catch (NoResultException e) {
            LOGGER.warnf("Failed to delete resource %s, scim mapping not found", id);
        }
    }

    public <M extends RoleMapperModel, S extends ResourceNode, A extends Adapter<M, S>> void refreshResources(
            Class<A> aClass,
            SynchronizationResult syncRes) {
        LOGGER.info("Refresh resources");
        LOGGER.debugf("Refreshing resources for %s", aClass.getSimpleName());
        getAdapter(aClass).getResourceStream().forEach(resource -> {
            var adapter = getAdapter(aClass);
            adapter.apply(resource);
            String resourceInfo = getResourceInfo(adapter);
            LOGGER.infof("Reconciling local resource %s: %s", adapter.getId(), resourceInfo);
            if (!adapter.skipRefresh()) {
                var mapping = adapter.getMapping();
                if (mapping == null) {
                    LOGGER.infof("Creating remote resource for %s", resourceInfo);
                    this.create(aClass, resource);
                    trackAdded(syncRes, adapter, resourceInfo);
                } else {
                    LOGGER.infof("Updating remote resource for %s", resourceInfo);
                    this.replace(aClass, resource);
                    trackUpdated(syncRes, adapter, resourceInfo);
                }
            } else {
                LOGGER.infof("Skipping refresh for %s", resourceInfo);
            }
        });

    }

    public <M extends RoleMapperModel, S extends ResourceNode, A extends Adapter<M, S>> void importResources(
            Class<A> aClass, SynchronizationResult syncRes) {
        LOGGER.info("Import");
        LOGGER.debugf("Importing resources for %s", aClass.getSimpleName());
        try {
            var adapter = getAdapter(aClass);
            ServerResponse<ListResponse<S>> response  = scimRequestBuilder.list("url", adapter.getResourceClass()).get().sendRequest();
            ListResponse<S> resourceTypeListResponse = response.getResource();

            for (var resource : resourceTypeListResponse.getListedResources()) {
                try {
                    LOGGER.infof("Reconciling remote resource %s", resource);
                    adapter = getAdapter(aClass);
                    adapter.apply(resource);

                    String resourceInfo = getResourceInfo(adapter);
                    LOGGER.infof("Processing remote resource: %s", resourceInfo);

                    var mapping = adapter.getMapping();
                    if (mapping != null) {
                        adapter.apply(mapping);
                        if (adapter.entityExists()) {
                            LOGGER.infof("Valid mapping found for %s, skipping", resourceInfo);
                            continue;
                        } else {
                            LOGGER.infof("Deleting dangling mapping for %s", resourceInfo);
                            adapter.deleteMapping();
                        }
                    }

                    var mapped = adapter.tryToMap();
                    if (mapped) {
                        LOGGER.infof("Matched local resource for %s", resourceInfo);
                        adapter.saveMapping();
                    } else {
                        switch (this.model.get("sync-import-action")) {
                            case "CREATE_LOCAL":
                                LOGGER.infof("Creating local resource for %s", resourceInfo);
                                try {
                                    adapter.createEntity();
                                    adapter.saveMapping();
                                    trackAdded(syncRes, adapter, resourceInfo);
                                } catch (Exception e) {
                                    LOGGER.errorf("Failed to create local resource for %s: %s", resourceInfo, e.getMessage());
                                    trackFailed(syncRes, adapter, resourceInfo + " (create failed: " + e.getMessage() + ")");
                                }
                                break;
                            case "DELETE_REMOTE":
                                LOGGER.infof("Deleting remote resource for %s", resourceInfo);
                                try {
                                    scimRequestBuilder
                                        .delete(genScimUrl(adapter.getSCIMEndpoint(),
                                                           resource.getId().get()),
                                                           adapter.getResourceClass())
                                        .sendRequest();
                                    trackRemoved(syncRes, adapter, resourceInfo);
                                } catch (Exception e) {
                                    LOGGER.errorf("Failed to delete remote resource for %s: %s", resourceInfo, e.getMessage());
                                    trackFailed(syncRes, adapter, resourceInfo + " (delete failed: " + e.getMessage() + ")");
                                }
                                break;
                        }
                    }
                } catch (Exception e) {
                    String resourceInfo = adapter != null ? getResourceInfo(adapter) : "unknown";
                    LOGGER.errorf("Failed to process resource %s: %s", resourceInfo, e.getMessage());
                    e.printStackTrace();
                    trackFailed(syncRes, adapter, resourceInfo + " (processing failed: " + e.getMessage() + ")");
                }
            }
        } catch (ResponseException e) {
            throw new RuntimeException(e);
        }
    }

    public <M extends RoleMapperModel, S extends ResourceNode, A extends Adapter<M, S>> void sync(Class<A> aClass,
            SynchronizationResult syncRes) {
        LOGGER.debugf("Starting sync for %s", aClass.getSimpleName());
        if (this.model.get("sync-import", false)) {
            this.importResources(aClass, syncRes);
        }
        if (this.model.get("sync-refresh", false)) {
            this.refreshResources(aClass, syncRes);
        }
        LOGGER.debugf("Sync completed for %s", aClass.getSimpleName());
    }

    public void close() {
        scimRequestBuilder.close();
    }

    private <M extends RoleMapperModel, A extends Adapter<M, ?>> String getResourceInfo(A adapter) {
        if (adapter instanceof UserAdapter userAdapter) {
            String username = userAdapter.getUsername();
            String email = userAdapter.getEmail();
            return String.format("User(username=%s, email=%s)", username, email);
        } else if (adapter instanceof GroupAdapter groupAdapter) {
            String displayName = groupAdapter.getDisplayName();
            return String.format("Group(name=%s, id=%s)", displayName, adapter.getId());
        }
        return String.format("Resource(id=%s)", adapter.getId());
    }

    private <M extends RoleMapperModel, A extends Adapter<M, ?>> void trackAdded(SynchronizationResult syncRes, A adapter, String resourceInfo) {
        if (syncRes instanceof ScimSynchronizationResult scimResult) {
            if (adapter instanceof UserAdapter) {
                scimResult.addAddedUser(resourceInfo);
            } else if (adapter instanceof GroupAdapter) {
                scimResult.addAddedGroup(resourceInfo);
            } else {
                syncRes.increaseAdded();
            }
        } else {
            syncRes.increaseAdded();
        }
    }

    private <M extends RoleMapperModel, A extends Adapter<M, ?>> void trackUpdated(SynchronizationResult syncRes, A adapter, String resourceInfo) {
        if (syncRes instanceof ScimSynchronizationResult scimResult) {
            if (adapter instanceof UserAdapter) {
                scimResult.addUpdatedUser(resourceInfo);
            } else if (adapter instanceof GroupAdapter) {
                scimResult.addUpdatedGroup(resourceInfo);
            } else {
                syncRes.increaseUpdated();
            }
        } else {
            syncRes.increaseUpdated();
        }
    }

    private <M extends RoleMapperModel, A extends Adapter<M, ?>> void trackRemoved(SynchronizationResult syncRes, A adapter, String resourceInfo) {
        if (syncRes instanceof ScimSynchronizationResult scimResult) {
            if (adapter instanceof UserAdapter) {
                scimResult.addRemovedUser(resourceInfo);
            } else if (adapter instanceof GroupAdapter) {
                scimResult.addRemovedGroup(resourceInfo);
            } else {
                syncRes.increaseRemoved();
            }
        } else {
            syncRes.increaseRemoved();
        }
    }

    private <M extends RoleMapperModel, A extends Adapter<M, ?>> void trackFailed(SynchronizationResult syncRes, A adapter, String resourceInfo) {
        if (syncRes instanceof ScimSynchronizationResult scimResult) {
            if (adapter instanceof UserAdapter) {
                scimResult.addFailedUser(resourceInfo);
            } else if (adapter instanceof GroupAdapter) {
                scimResult.addFailedGroup(resourceInfo);
            } else {
                syncRes.increaseFailed();
            }
        } else {
            syncRes.increaseFailed();
        }
    }
}
