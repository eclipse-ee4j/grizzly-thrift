/*
 * Copyright (c) 2012, 2017 Oracle and/or its affiliates. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v. 2.0, which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the
 * Eclipse Public License v. 2.0 are satisfied: GNU General Public License,
 * version 2 with the GNU Classpath Exception, which is available at
 * https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 */

package org.glassfish.grizzly.thrift.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Returns the stored value by round-robin
 * <p>
 * All operations can be called dynamically and are thread-safe.
 *
 * @author Bongjae Chang
 */
public class RoundRobinStore<T> {

    private final List<T> valueList = new ArrayList<T>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Lock rLock = lock.readLock();
    private final Lock wLock = lock.writeLock();
    private int counter = 0;

    public RoundRobinStore() {
    }

    public RoundRobinStore(final Set<T> initSet, final boolean shuffle) {
        configure(initSet, shuffle);
    }

    public void configure(final Set<T> initSet, final boolean shuffle) {
        if (initSet != null && !initSet.isEmpty()) {
            wLock.lock();
            try {
                valueList.addAll(initSet);
                if (shuffle) {
                    Collections.shuffle(valueList);
                }
            } finally {
                wLock.unlock();
            }
        }
    }

    public void shuffle() {
        wLock.lock();
        try {
            if (!valueList.isEmpty()) {
                Collections.shuffle(valueList);
            }
        } finally {
            wLock.unlock();
        }
    }

    public void add(final T value) {
        wLock.lock();
        try {
            valueList.add(value);
        } finally {
            wLock.unlock();
        }
    }

    public void remove(final T value) {
        wLock.lock();
        try {
            valueList.remove(value);
        } finally {
            wLock.unlock();
        }
    }

    public T get() {
        rLock.lock();
        try {
            final int valueSize = valueList.size();
            if (valueSize <= 0) {
                return null;
            }
            final int index = (counter++ & 0x7fffffff) % valueSize;
            return valueList.get(index);
        } finally {
            rLock.unlock();
        }
    }

    public boolean hasValue(final T value) {
        if (value == null) {
            return false;
        }
        rLock.lock();
        try {
            return valueList.contains(value);
        } finally {
            rLock.unlock();
        }
    }

    public boolean hasOnly(final T value) {
        if (value == null) {
            return false;
        }
        rLock.lock();
        try {
            return valueList.size() == 1 && valueList.contains(value);
        } finally {
            rLock.unlock();
        }
    }

    public int size() {
        rLock.lock();
        try {
            return valueList.size();
        } finally {
            rLock.unlock();
        }

    }

    public void clear() {
        wLock.lock();
        try {
            valueList.clear();
        } finally {
            wLock.unlock();
        }
    }
}
