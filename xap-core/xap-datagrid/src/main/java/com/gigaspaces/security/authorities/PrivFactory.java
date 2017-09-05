package com.gigaspaces.security.authorities;

import java.util.HashMap;
import java.util.Map;

/**
 * @author livnat
 * @since 12.2
 */
public class PrivFactory {
    private final static Map gridPrivilegeMap = new HashMap();
    private final static Map monitorPrivilegeMap = new HashMap();
    private final static Map systemPrivilegeMap = new HashMap();

    static {

        for (GridAuthority.GridPrivilege gridPrivilege : GridAuthority.GridPrivilege.values()) {
            gridPrivilegeMap.put(gridPrivilege.name(), gridPrivilege);
        }
        for (MonitorAuthority.MonitorPrivilege monitorPrivilege : MonitorAuthority.MonitorPrivilege.values()) {
            monitorPrivilegeMap.put(monitorPrivilege.name(), monitorPrivilege);
        }
        for (SystemAuthority.SystemPrivilege systemPrivilege : SystemAuthority.SystemPrivilege.values()) {
            systemPrivilegeMap.put(systemPrivilege.name(), systemPrivilege);
        }
    }

    public static Privilege create(String s) {

        if(gridPrivilegeMap.containsKey(s)) {
            return (Privilege) gridPrivilegeMap.get(s);
        }
        else if(monitorPrivilegeMap.containsKey(s)){
            return (Privilege) monitorPrivilegeMap.get(s);
        }
        else if(systemPrivilegeMap.containsKey(s)){
            return (Privilege) systemPrivilegeMap.get(s);
        }
        return null;
    }


}