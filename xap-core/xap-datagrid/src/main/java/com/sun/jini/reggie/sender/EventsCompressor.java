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

import com.sun.jini.reggie.GigaRegistrar;
import com.sun.jini.reggie.RegistrarEvent;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceRegistrar;

import java.util.ArrayList;
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
            if (oldTransition == 1) {
                if (currentTransition == 1) {
                    throw new IllegalStateException("compressing event " + event + " onto " + events);
                } else if (currentTransition == 2) {
                    events.remove(index);
                } else if (currentTransition == 4) {
                    throw new IllegalStateException("compressing event " + event + " onto " + events);
                } else {
                    throw new IllegalStateException("compressing event " + event + " onto " + events);
                }
            } else if (oldTransition == 2) {
                if (currentTransition == 1) {
                    events.remove(index);
                } else if (currentTransition == 2) {
                    throw new IllegalStateException("compressing event " + event + " onto " + events);
                } else if (currentTransition == 4) {
                    event.setTransition(4);
                    events.set(index, event);
                } else {
                    throw new IllegalStateException("compressing event " + event + " onto " + events);
                }

            } else if (oldTransition == 4) {
                if (currentTransition == 1) {
                    events.set(index, event);
                } else if (currentTransition == 2) {
                    throw new IllegalStateException("compressing event " + event + " onto " + events);
                } else if (currentTransition == 4) {
                    events.set(index, event);
                } else {
                    throw new IllegalStateException("compressing event " + event + " onto " + events);
                }
            } else {
                throw new IllegalStateException("compressing event " + event + " onto " + events);
            }
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

    private static final Logger logger = Logger.getLogger(EventsCompressor.class.getName());
    public static void compress(ArrayList<GigaRegistrar.EventTask> eventTasks) {
        if (eventTasks.size() == 1) {
            return;
        }

        GigaRegistrar.EventTask current = eventTasks.remove(eventTasks.size() -1); //remove last event
        GigaRegistrar.EventTask candidate = null;
        int candidateIndex = -1;
        for (int i=eventTasks.size() -1; i>=0; --i) {
            if (eventTasks.get(i).sid.equals(current.sid)) {
                candidate = eventTasks.get(i);
                candidateIndex = i;
                break;
            }
        }

        if (candidate == null) {
            logger.severe("No candidate was found, and skipped event: #"+current.seqNo+"-"+current.reg);
            throw new IllegalStateException("No candidate was found, and skipped event: #"+current.seqNo+"-"+current.reg);
        }

        final int currentTransition = current.transition;
        final int oldTransition = candidate.transition;
        if (oldTransition == MATCH_NOMATCH) {
            if (currentTransition == MATCH_NOMATCH) {
                throwISE(MATCH_NOMATCH, candidate, MATCH_NOMATCH, current);
            } else if (currentTransition == NOMATCH_MATCH) {
                eventTasks.remove(candidateIndex);
                logTransition(MATCH_NOMATCH, candidate, NOMATCH_MATCH, current);
                //TODO why aren't we adding current?
            } else if (currentTransition == MATCH_MATCH) {
                throwISE(MATCH_NOMATCH, candidate, MATCH_MATCH, current);
            } else {
                throwISE(MATCH_NOMATCH, candidate, -1, current);
            }
        } else if (oldTransition == NOMATCH_MATCH) {
            if (currentTransition == MATCH_NOMATCH) {
                eventTasks.remove(candidateIndex);
                logTransition(NOMATCH_MATCH, candidate, MATCH_NOMATCH, current);
            } else if (currentTransition == NOMATCH_MATCH) {
                throwISE(NOMATCH_MATCH, candidate, NOMATCH_MATCH, current);
            } else if (currentTransition == MATCH_MATCH) {
                current.transition = MATCH_MATCH;
                eventTasks.set(candidateIndex, current);
                logTransition(NOMATCH_MATCH, candidate, MATCH_MATCH, current);
            } else {
                throwISE(NOMATCH_MATCH, candidate, -1, current);
            }

        } else if (oldTransition == MATCH_MATCH) {
            if (currentTransition == MATCH_NOMATCH) {
                eventTasks.set(candidateIndex, current);
                logTransition(MATCH_MATCH, candidate, MATCH_NOMATCH, current);
            } else if (currentTransition == NOMATCH_MATCH) {
                throwISE(MATCH_MATCH, candidate, NOMATCH_MATCH, current);
            } else if (currentTransition == MATCH_MATCH) {
                eventTasks.set(candidateIndex, current);
                logTransition(MATCH_MATCH, candidate, MATCH_MATCH, current);
            } else {
                throwISE(MATCH_MATCH, candidate, -1, current);
            }
        } else {
            throwISE(-1, candidate, -1, current);
        }

    }

    private static void logTransition(int oldTransition, GigaRegistrar.EventTask candidate, int currTransition, GigaRegistrar.EventTask current) {
        logger.info(translateTransition(oldTransition, candidate, currTransition, current));
    }

    private static void throwISE(int oldTransition, GigaRegistrar.EventTask candidate, int currTransition, GigaRegistrar.EventTask current) {
        throw new IllegalStateException("illegal transition: " + translateTransition(oldTransition, candidate, currTransition, current));
    }

    private static String translateTransition(int oldTransition, GigaRegistrar.EventTask candidate, int currTransition, GigaRegistrar.EventTask current) {
        return translate(oldTransition)+"-> #"+candidate.sid+"-"+candidate.reg
                +" ... " + translate(currTransition)+"-> #"+current.sid+"-"+current.reg;
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
