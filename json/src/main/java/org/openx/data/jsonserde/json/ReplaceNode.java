package org.openx.data.jsonserde.json;

import java.util.*;

public class ReplaceNode {

    public String replaceWith = null;
    public final Map<String, ReplaceNode> children = new HashMap<String, ReplaceNode>();

    public ReplaceNode nextNode(String keyToMatch) {
        if(keyToMatch == null) {
            return null;
        }
        String normKeyToMatch = keyToMatch.toLowerCase();

        /* NOTE!
            Assure that the longest key always gets a chance to match first.
            This will assure that the pattern "abc_*" matches before "a*" which will
            almost always be what's desired.
          */
        List<Map.Entry<String, ReplaceNode>> orderedEntries = new ArrayList<Map.Entry<String, ReplaceNode>>(children.entrySet());
        Collections.sort(
                orderedEntries,
                new Comparator<Map.Entry<String, ReplaceNode>>() {
                    @Override
                    public int compare(Map.Entry<String, ReplaceNode> o1, Map.Entry<String, ReplaceNode> o2) {
                        Integer o1KeyLen = o1.getKey().length();
                        Integer o2KeyLen = o2.getKey().length();
                        return -o1KeyLen.compareTo(o2KeyLen);
                    }
                }
        );

        for(Map.Entry<String, ReplaceNode> entry : orderedEntries) {
            String key = entry.getKey();
            ReplaceNode node = entry.getValue();
            if(key.endsWith("*")) {
                if(normKeyToMatch.length() < (key.length()-1)) {
                    continue;
                }
                String keyPart = key.substring(0, key.length()-1);
                String normKeyToMatchPart = normKeyToMatch.substring(0, key.length()-1);
                if(keyPart.equalsIgnoreCase(normKeyToMatchPart)) {
                    return node;
                }
            }
            else if(key.equalsIgnoreCase(normKeyToMatch)) {
                return node;
            }
        }

        return null;
    }

}