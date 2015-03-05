package com.conveyal.osmlib;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;

public class Relation extends OSMEntity implements Serializable {

    private static final long serialVersionUID = 1L;

//    Node nodes;
//    Way ways;
//    Relation relations;

    public List<Member> members = Lists.newArrayList();
    
    public static class Member implements Serializable {
        private static final long serialVersionUID = 1L;
        public Type type;
        public long id;
        public String role;
        @Override
        public String toString() {
            return String.format("%s %s %d", role, type.toString(), id);
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Relation with tags ");
        sb.append(tags);
        sb.append('\n');
        for (Member member : members) {
            sb.append("  ");
            sb.append(member.toString());
            sb.append('\n');
        }
        sb.append('\n');
        return sb.toString();
    }

	@Override
	public Type getType() {
		return OSMEntity.Type.RELATION;
	}
}