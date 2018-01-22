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

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Bongjae Chang
 */
public class RoundRobinStoreTest {

    @Test
    public void testBasicRoundRobin() {
        final RoundRobinStore<Integer> roundRobin = new RoundRobinStore<Integer>();
        for (int i = 0; i < 50; i++) {
            roundRobin.add(i);
        }
        for (int i = 0; i < 100; i++) {
            final Integer value = roundRobin.get();
            Assert.assertNotNull("value should not be null", value);
            Assert.assertTrue("invalid value", i % 50 == value);
        }
    }

    @Test
    public void testDuplication() {
        final Set<Integer> init = new HashSet<Integer>();
        for (int i = 0; i < 50; i++) {
            init.add(i);
        }
        final RoundRobinStore<Integer> roundRobin = new RoundRobinStore<Integer>(init, true);
        final Set<Integer> checkedSet = new HashSet<Integer>();
        for (int i = 0; i < 50; i++) {
            checkedSet.add(roundRobin.get());
        }
        Assert.assertTrue(checkedSet.size() == 50);
    }
}
