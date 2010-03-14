/*
 * Copyright 2007-2010 Amazon Technologies, Inc. or its affiliates.
 * Amazon, Amazon.com and Carbonado are trademarks or registered trademarks
 * of Amazon Technologies, Inc. or its affiliates.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazon.carbonado.qe;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.info.StorableIndex;

import com.amazon.carbonado.stored.StorableTestBasic;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestCompositeScore extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestCompositeScore.class);
    }

    public TestCompositeScore(String name) {
        super(name);
    }

    public void testRowRangeHint() throws Exception {
        StorableIndex<StorableTestBasic> ix_1, ix_2;
        ix_1 = TestFilteringScore.makeIndex(StorableTestBasic.class, "id").clustered(true);
        ix_2 = TestFilteringScore.makeIndex(StorableTestBasic.class, "stringProp");

        OrderingList<StorableTestBasic> ordering = TestOrderingScore
            .makeOrdering(StorableTestBasic.class, "stringProp");

        CompositeScore<StorableTestBasic> score_1 = CompositeScore.evaluate(ix_1, null, ordering);
        CompositeScore<StorableTestBasic> score_2 = CompositeScore.evaluate(ix_2, null, ordering);

        int result_1 = CompositeScore.fullComparator().compare(score_1, score_2);

        // Favor full scan and sort.
        assertTrue(result_1 < 0);

        QueryHints hints = QueryHints.emptyHints().with(QueryHint.CONSUME_SLICE);

        int result_2 = CompositeScore.fullComparator(hints).compare(score_1, score_2);

        // Favor ordered index.
        assertTrue(result_2 > 0);

        hints = hints.without(QueryHint.CONSUME_SLICE);

        int result_3 = CompositeScore.fullComparator(hints).compare(score_1, score_2);

        // Favor full scan without hint.
        assertTrue(result_3 < 0);
    }
}
