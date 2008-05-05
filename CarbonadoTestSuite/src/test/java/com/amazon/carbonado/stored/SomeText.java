/*
 * Copyright 2008 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.stored;

import com.amazon.carbonado.*;
import com.amazon.carbonado.adapter.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@PrimaryKey("id")
public interface SomeText extends Storable {
    int getId();
    void setId(int id);

    @TextAdapter(charset="UTF-8")
    @Nullable
    String getText1();
    void setText1(String text);

    @TextAdapter(altCharsets={}, charset="UTF-8")
    @Nullable
    String getText2();
    void setText2(String text);

    @TextAdapter(altCharsets={"ASCII"}, charset="UTF-8")
    @Nullable
    String getText3();
    void setText3(String text);

    @TextAdapter(altCharsets={"ASCII", "UTF-16"}, charset="UTF-8")
    @Nullable
    String getText4();
    void setText4(String text);

    @TextAdapter(altCharsets={})
    @Nullable
    String getText5();
    void setText5(String text);

    @TextAdapter(altCharsets="ASCII")
    @Nullable
    String getText6();
    void setText6(String text);

    @TextAdapter(altCharsets={"ASCII", "UTF-16"})
    @Nullable
    String getText7();
    void setText7(String text);
}
