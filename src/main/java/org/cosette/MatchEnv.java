package org.cosette;

import kala.collection.Map;
import kala.collection.Set;

public record MatchEnv(Map<Integer, Set<Integer>> columnMap) {

    public static MatchEnv empty() {
        return new MatchEnv(Map.empty());
    }

    public boolean verify() {
        return false;
    }

}
