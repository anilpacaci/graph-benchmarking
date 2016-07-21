package ca.uwaterloo.cs.ldbc.interactive.gremlin;

/**
 * Created by apacaci on 7/21/16.
 */
public enum Entity {
    COMMENT("comment"),
    FORUM("forum"),
    MESSAGE("message"),
    ORGANISATION("organisation"),
    PERSON("person"),
    PLACE("place"),
    POST("post"),
    TAG("tag"),
    TAGCLASS("tagclass");

    private final String name;

    private Entity(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

}
