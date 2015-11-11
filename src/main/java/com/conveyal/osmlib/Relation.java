package com.conveyal.osmlib;

import com.google.common.collect.Lists;
import org.mapdb.DataIO;
import org.mapdb.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class Relation extends OSMEntity implements Serializable {

    private static final long serialVersionUID = 1L;

    public List<Member> members = Lists.newArrayList();

    public static class Member implements Serializable {

        private static final long serialVersionUID = 1L;
        public OSMEntity.Type type;
        public long id;
        public String role;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(type.toString());
            sb.append(' ');
            sb.append(id);
            if (role != null && !role.isEmpty()) {
                sb.append(" as ");
                sb.append(role);
            }
            return sb.toString();
        }

        @Override
        public boolean equals (Object other) {
            if (!(other instanceof Member)) return false;
            Member otherMember = (Member) other;
            return this.type == otherMember.type &&
                   this.id == otherMember.id &&
                   this.role.equals(otherMember.role);
        }

    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("relation with tags ");
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
    public boolean equals(Object other) {
        if ( ! (other instanceof Relation)) return false;
        Relation otherRelation = (Relation) other;
        return this.members.equals(otherRelation.members) && this.tagsEqual(otherRelation);
    }

    @Override
    public Type getType() {
        return Type.RELATION;
    }


    //TODO field 'tags' from parent class 'OSMEntity'
    protected static final class RelationSerializer extends Serializer<Relation> implements Serializable{
        @Override
        public void serialize(DataOutput out, Relation value) throws IOException {
            DataIO.packInt(out, value.members.size());
            for(Member member:value.members){
                DataIO.packInt(out, member.type.ordinal());
                DataIO.packLong(out, member.id);
                out.writeUTF(member.role);
            }
        }

        @Override
        public Relation deserialize(DataInput in, int available) throws IOException {

            Relation ret = new Relation();
            int size = DataIO.unpackInt(in);
            for(int i=0;i<size;i++){
                int typeInt = DataIO.unpackInt(in);
                Member member = new Member();
                member.type = OSMEntity.Type.values()[typeInt];
                member.id = DataIO.unpackLong(in);
                member.role = in.readUTF();

                ret.members.add(member);
            }
            return ret;
        }
    }


}
