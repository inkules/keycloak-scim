package sh.libre.scim.core;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import jakarta.persistence.NoResultException;

import de.captaingoldfish.scim.sdk.client.ScimRequestBuilder;
import de.captaingoldfish.scim.sdk.client.builder.PatchBuilder;
import de.captaingoldfish.scim.sdk.common.constants.enums.PatchOp;
import de.captaingoldfish.scim.sdk.common.resources.Group;
import de.captaingoldfish.scim.sdk.common.resources.multicomplex.Member;
import de.captaingoldfish.scim.sdk.common.resources.complex.Meta;
import org.apache.commons.lang3.StringUtils;
import org.jboss.logging.Logger;
import org.keycloak.models.GroupModel;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.UserModel;

public class GroupAdapter extends Adapter<GroupModel, Group> {

    private String displayName;
    private Set<String> members = new HashSet<String>();

    public GroupAdapter(KeycloakSession session, String componentId) {
        super(session, componentId, "Group", Logger.getLogger(GroupAdapter.class));
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        if (this.displayName == null) {
            this.displayName = displayName;
        }
    }

    @Override
    public Class<Group> getResourceClass() {
        return Group.class;
    }

    @Override
    public void apply(GroupModel group) {
        setId(group.getId());
        setDisplayName(group.getName());
        this.members = session.users()
                .getGroupMembersStream(session.getContext().getRealm(), group)
                .map(x -> x.getId())
                .collect(Collectors.toSet());
        this.skip = StringUtils.equals(group.getFirstAttribute("scim-skip"), "true");
    }

    @Override
    public void apply(Group group) {
        setExternalId(group.getId().get());
        setDisplayName(group.getDisplayName().get());
        var groupMembers = group.getMembers();
        if (groupMembers != null && groupMembers.size() > 0) {
            this.members = new HashSet<String>();
            for (var groupMember : groupMembers) {
                var databricksUserId = groupMember.getValue().get();
                try {
                    // Find the Keycloak user by Databricks user ID (externalId)
                    var userMapping = query("findByExternalId", databricksUserId, "User");
                    var mapping = userMapping.getSingleResult();
                    this.members.add(mapping.getId());
                } catch (Exception e) {
                    LOGGER.warn("Could not find user mapping for Databricks user ID: " + databricksUserId, e);
                }
            }
        }
    }

    @Override
    public Group toSCIM(Boolean addMeta) {
        var group = new Group();
        group.setId(externalId);
        group.setExternalId(id);
        group.setDisplayName(displayName);
        if (members.size() > 0) {
            var groupMembers = new ArrayList<Member>();
            for (var member : members) {
                var groupMember = new Member();
                try {
                    var user = session.users().getUserById(realm, member);
                    if (user != null) {
                        // Get the Databricks user ID from the mapping
                        var userMapping = query("findById", user.getId(), "User");
                        var mapping = userMapping.getSingleResult();
                        String databricksUserId = mapping.getExternalId();
                        groupMember.setValue(databricksUserId);
                        var ref = new URI(String.format("Users/%s", databricksUserId));
                        groupMember.setRef(ref.toString());
                        groupMembers.add(groupMember);
                    }
                } catch (Exception e) {
                    LOGGER.error(e);
                }
            }
            group.setMembers(groupMembers);
        }
        if (addMeta) {
            var meta = new Meta();
            try {
                var uri = new URI("Groups/" + externalId);
                meta.setLocation(uri.toString());
            } catch (URISyntaxException e) {
            }
            group.setMeta(meta);
        }
        return group;
    }

    @Override
    public Boolean entityExists() {
        if (this.id == null) {
            return false;
        }
        var group = session.groups().getGroupById(realm, id);
        if (group != null) {
            return true;
        }
        return false;
    }

    @Override
    public Boolean tryToMap() {
        var group = session.groups().getGroupsStream(realm).filter(x -> x.getName() == displayName).findFirst();
        if (group.isPresent()) {
            setId(group.get().getId());
            return true;
        }
        return false;
    }

    @Override
    public void createEntity() {
        var group = session.groups().createGroup(realm, displayName);
        this.id = group.getId();
        for (String mId : members) {
            try {
                var user = session.users().getUserById(realm, mId);
                if (user == null) {
                    throw new NoResultException();
                }
                user.joinGroup(group);
            } catch (Exception e) {
                LOGGER.warn(e);
            }
        }
    }

    @Override
    public Stream<GroupModel> getResourceStream() {
        return getFilteredGroups();
    }

    @Override
    public Boolean skipRefresh() {
        return false;
    }

    @Override
    public PatchBuilder<Group> toPatchBuilder(ScimRequestBuilder scimRequestBuilder, String url) {
        List<Member> groupMembers = new ArrayList<>();
        PatchBuilder<Group> patchBuilder;
        patchBuilder = scimRequestBuilder.patch(url, Group.class);
        if (members.size() > 0) {
            for (String member : members) {
                var user = session.users().getUserById(realm, member);
                if (user != null) {
                    try {
                        // Get the Databricks user ID from the mapping
                        var userMapping = query("findById", user.getId(), "User");
                        var mapping = userMapping.getSingleResult();
                        String databricksUserId = mapping.getExternalId();
                        groupMembers.add(Member.builder().value(databricksUserId).build());
                    } catch (Exception e) {
                        LOGGER.error("Failed to get mapping for user " + user.getId(), e);
                    }
                }
            }
            patchBuilder.addOperation()
                .path("members")
                .op(PatchOp.REPLACE)
                .valueNodes(groupMembers)
                .build();
        } else {
            patchBuilder.addOperation()
                .path("members")
                .op(PatchOp.REMOVE)
                .value(null)
                .build();
        }
        LOGGER.info(patchBuilder.getResource());
        return patchBuilder;
    }
}
