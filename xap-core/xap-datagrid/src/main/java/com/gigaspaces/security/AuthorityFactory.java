/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.gigaspaces.security;

import com.gigaspaces.security.authorities.Constants;
import com.gigaspaces.security.authorities.GridAuthority;
import com.gigaspaces.security.authorities.GridAuthority.GridPrivilege;
import com.gigaspaces.security.authorities.MonitorAuthority;
import com.gigaspaces.security.authorities.MonitorAuthority.MonitorPrivilege;
import com.gigaspaces.security.authorities.RoleAuthority;
import com.gigaspaces.security.authorities.RoleAuthority.RolePrivilege;
import com.gigaspaces.security.authorities.SpaceAuthority;
import com.gigaspaces.security.authorities.SpaceAuthority.SpacePrivilege;
import com.gigaspaces.security.authorities.SystemAuthority;
import com.gigaspaces.security.authorities.SystemAuthority.SystemPrivilege;

/**
 * A factory for creating an {@link Authority} instance back from its String representation returned
 * by {@link Authority#getAuthority()}
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
public final class AuthorityFactory {

    /**
     * Creates an {@link Authority} instance out of its String representation {@link
     * Authority#getAuthority()}.
     *
     * @param authority An authority String representation.
     * @return An authority instance.
     */
    public static Authority create(String authority) {
        String[] split = authority.split(Constants.DELIM);
        if (split.length < 2) {
            throw new IllegalArgumentException("Illegal Authority format: " + authority);
        }
        String privilege = split[Constants.PRIVILEGE_NAME_POS];
        if (RolePrivilege.class.getSimpleName().equals(privilege)) {
            return RoleAuthority.valueOf(authority);
        } else if (SystemPrivilege.class.getSimpleName().equals(privilege)) {
            return SystemAuthority.valueOf(authority);
        } else if (SpacePrivilege.class.getSimpleName().equals(privilege)) {
            return SpaceAuthority.valueOf(authority);
        } else if (GridPrivilege.class.getSimpleName().equals(privilege)) {
            return GridAuthority.valueOf(authority);
        } else if (MonitorPrivilege.class.getSimpleName().equals(privilege)) {
            return MonitorAuthority.valueOf(authority);
        }

        throw new IllegalArgumentException("Unknown authority type; Could not create an Authority from: " + authority);
    }
}
