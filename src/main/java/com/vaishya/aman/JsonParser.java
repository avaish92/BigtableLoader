package com.vaishya.aman;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class JsonParser {
    public static Map<String,ArrayList> responseFeeder(){
        Map<String,ArrayList> map = new Map<String, ArrayList>() {
            @Override
            public int size() {
                return 0;
            }

            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public boolean containsKey(Object key) {
                return false;
            }

            @Override
            public boolean containsValue(Object value) {
                return false;
            }

            @Override
            public ArrayList get(Object key) {
                return null;
            }

            @Override
            public ArrayList put(String key, ArrayList value) {
                return null;
            }

            @Override
            public ArrayList remove(Object key) {
                return null;
            }

            @Override
            public void putAll(Map<? extends String, ? extends ArrayList> m) {

            }

            @Override
            public void clear() {

            }

            @Override
            public Set<String> keySet() {
                return null;
            }

            @Override
            public Collection<ArrayList> values() {
                return null;
            }

            @Override
            public Set<Entry<String, ArrayList>> entrySet() {
                return null;
            }
        };
        return map;
    }
}
