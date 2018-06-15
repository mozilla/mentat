/* -*- Mode: Java; c-basic-offset: 4; tab-width: 20; indent-tabs-mode: nil; -*-
 * Copyright 2018 Mozilla
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License. */

package org.mozilla.mentat;

import com.sun.jna.Structure;
import com.sun.jna.ptr.IntByReference;

import java.util.Arrays;
import java.util.List;

/**
 * Represents a C struct of a list of Strings containing attributes in the format
 * `:namespace/name`.
 */
public class AttributeList extends Structure {
    public static class ByReference extends AttributeList implements Structure.ByReference {
    }

    public static class ByValue extends AttributeList implements Structure.ByValue {
    }

    public IntByReference attributes;
    public int numberOfItems;
    // Used by the Swift counterpart, JNA does this for us automagically.
    // But we still need it here so that the number of fields and their order is correct
    public int len;

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("attributes", "numberOfItems", "len");
    }

    // TODO: where is this object returned, who owns it's memory, who should be freeing it?
}
