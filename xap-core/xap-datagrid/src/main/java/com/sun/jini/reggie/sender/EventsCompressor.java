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
package com.sun.jini.reggie.sender;

import com.sun.jini.reggie.RegistrarEvent;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceRegistrar;

import java.util.List;
import java.util.logging.Logger;

/**
 * Created by Barak Bar Orion 7/7/15.
 */
@com.gigaspaces.api.InternalApi
public class EventsCompressor {

    final static int MATCH_NOMATCH = ServiceRegistrar.TRANSITION_MATCH_NOMATCH;
    final static int NOMATCH_MATCH = ServiceRegistrar.TRANSITION_NOMATCH_MATCH;
    final static int MATCH_MATCH   = ServiceRegistrar.TRANSITION_MATCH_MATCH;

    private static final Logger logger = Logger.getLogger(EventsCompressor.class.getName());

    public static void compress(List<RegistrarEvent> events, RegistrarEvent event) {
        if (events.isEmpty()) {
            events.add(event);
            return;
        }
        int index = findEventWithServiceId(event.getServiceID(), events);
        if (-1 < index) {
            RegistrarEvent old = events.get(index);
            int oldTransition = old.getTransition();
            int currentTransition = event.getTransition();
            if (oldTransition == MATCH_NOMATCH) {
                if (currentTransition == MATCH_NOMATCH) {
                    throwISE(MATCH_NOMATCH, old, MATCH_NOMATCH, event);
                } else if (currentTransition == NOMATCH_MATCH) {
                    events.remove(index);
                    logTransition(MATCH_NOMATCH, old, NOMATCH_MATCH, event, "removed->lost"); //IMO, we should be adding current event to the list
                } else if (currentTransition == MATCH_MATCH) {
                    throwISE(MATCH_NOMATCH, old, MATCH_MATCH, event);
                } else {
                    throwISE(MATCH_NOMATCH, old, -1, event);
                }
            } else if (oldTransition == NOMATCH_MATCH) {
                if (currentTransition == MATCH_NOMATCH) {
                    events.remove(index);
                    logTransition(NOMATCH_MATCH, old, MATCH_NOMATCH, event, "added->removed");
                } else if (currentTransition == NOMATCH_MATCH) {
                    throwISE(NOMATCH_MATCH, old, NOMATCH_MATCH, event);
                } else if (currentTransition == MATCH_MATCH) {
                    event.setTransition(MATCH_MATCH);
                    events.set(index, event);
                    logTransition(NOMATCH_MATCH, old, MATCH_MATCH, event, "added->modified");
                } else {
                    throwISE(NOMATCH_MATCH, old, -1, event);
                }

            } else if (oldTransition == MATCH_MATCH) {
                if (currentTransition == MATCH_NOMATCH) {
                    events.set(index, event);
                    logTransition(MATCH_MATCH, old, MATCH_NOMATCH, event, "modified->removed");
                } else if (currentTransition == NOMATCH_MATCH) {
                    throwISE(MATCH_MATCH, old, NOMATCH_MATCH, event);
                } else if (currentTransition == MATCH_MATCH) {
                    events.set(index, event);
                    logTransition(MATCH_MATCH, old, MATCH_MATCH, event, "modified->modified");
                } else {
                    throwISE(MATCH_MATCH, old, -1, event);
                }
            } else {
                throwISE(-1, old, -1, event);
            }
        } else {
            logger.severe("No candidate was found, and skipped event: #"+event.getSequenceNumber()+"-"+event);
        }

    }

    private static int findEventWithServiceId(ServiceID serviceID, List<RegistrarEvent> events) {
        int size = events.size();
        for (int i = size - 1; -1 < i; --i) {
            RegistrarEvent candidate = events.get(i);
            if (candidate.getServiceID().equals(serviceID)) {
                return i;
            }
        }
        return -1;
    }

    private static void logTransition(int oldTransition, RegistrarEvent candidate, int currTransition, RegistrarEvent current, String action) {
        logger.info(translateTransition(oldTransition, candidate, currTransition, current) + " (" + action + ")");
    }

    private static void throwISE(int oldTransition, RegistrarEvent candidate, int currTransition, RegistrarEvent current) {
        throw new IllegalStateException("illegal transition: " + translateTransition(oldTransition, candidate, currTransition, current));
    }

    private static String translateTransition(int oldTransition, RegistrarEvent candidate, int currTransition, RegistrarEvent current) {
        return "svcId: " + current.getServiceID()
                + " #" + candidate.getSequenceNumber() + "-" + translate(oldTransition)
                + "-> #" + current.getSequenceNumber() + "-" + translate(currTransition);
    }

    public static String translate(int value) {
        if(value == 1){
            return "MATCH_NOMATCH";
        }else if(value == 2){
            return "NOMATCH_MATCH";
        }else if(value== 4){
            return "MATCH_MATCH";
        }
        return String.valueOf(value);
    }
}
