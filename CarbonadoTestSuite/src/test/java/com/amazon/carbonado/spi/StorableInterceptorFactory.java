/*
 * Copyright 2006 Amazon Technologies, Inc. or its affiliates.
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
package com.amazon.carbonado.spi;

import java.util.Map;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;

import org.cojen.util.ClassInjector;
import org.cojen.util.WeakIdentityMap;
import org.cojen.classfile.TypeDesc;
import org.cojen.classfile.ClassFile;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.CodeBuilder;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.spi.CodeBuilderUtil;

/**
 * StorableInterceptorFactory creates instances of Storables that delegate
 * calls to a proxy.
 *
 * <p>If the base class for the interceptor is abstract and has any methods implemented, those
 * methods will be invoked directly and no further action taken.
 *
 * <p>Any methods which are not implemented will be delegated to the proxy which is provided to the
 * constructor.
 *
 * @author Don Schneider
 */
public class StorableInterceptorFactory<S extends Storable> {

    public static final String PROXY = "mProxy$";

    private static Map<Object, Reference<StorableInterceptorFactory>>
            cCache = new WeakIdentityMap();

    /**
     * @param interceptorType the handler type to be invoked for accessors and
     * mutators, which should just be the type of S.
     */
    public static <S extends Storable> StorableInterceptorFactory<S> getInstance(
            Class<? extends S> interceptorType, Class<S> userType, boolean shortCircuit)
    {
        synchronized (cCache) {
            StorableInterceptorFactory<S> factory;
            String key = interceptorType.getName() + userType.getName() + (shortCircuit?"S":"P");
            Reference<StorableInterceptorFactory> ref = cCache.get(key);
            if (null != ref) {
                factory = ref.get();
                if (factory != null) {
                    return factory;
                }
            }
            factory = new StorableInterceptorFactory<S>(interceptorType, userType, shortCircuit);
            cCache.put(key, new SoftReference<StorableInterceptorFactory>(factory));
            return factory;
        }
    }

    private final Constructor<? extends S> mConstructor;

    private StorableInterceptorFactory(final Class<? extends S> interceptorType,
                                       Class<S> userType,
                                       boolean doShortCircuit) {
        Class storableClass = generateStorable(interceptorType, userType, doShortCircuit);
        try {
            mConstructor = storableClass.getConstructor(userType);
        }
        catch (NoSuchMethodException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    private static <T extends Storable> Class<? extends T>
            generateStorable(Class<? extends T> interceptorType,
                             Class<T> userType,
                             boolean doShortCircuit)
    {
        TypeDesc interceptorTypeDesc = TypeDesc.forClass(interceptorType);
        TypeDesc userTypeDesc = TypeDesc.forClass(userType);

        ClassInjector ci = ClassInjector.create(interceptorType.getName(), null);
        ClassFile cf = CodeBuilderUtil.createStorableClassFile(
                ci,
                interceptorType,
                false,
                StorableInterceptorFactory.class.getName());

        // private final Storable mProxy$;
        cf.addField(Modifiers.PRIVATE.toFinal(true), PROXY, userTypeDesc);

        final TypeDesc[] ctorParams = {userTypeDesc};
        // Add public constructor:
        {
            final int storableHandler = 0;
            MethodInfo mi = cf.addConstructor(Modifiers.PUBLIC, ctorParams);
            CodeBuilder b = new CodeBuilder(mi);
            b.loadThis();
            try {
                interceptorType.getConstructor(new Class[] {userType});
                b.loadLocal(b.getParameter(storableHandler));
                b.invokeSuperConstructor(ctorParams);
            }
            catch (NoSuchMethodException e) {
                b.invokeSuperConstructor(null);
            }


            //// this.storableHandler = storableHandler
            CodeBuilderUtil.assertParameterNotNull(b, storableHandler);
            b.loadThis();
            b.loadLocal(b.getParameter(storableHandler));
            b.storeField(PROXY, userTypeDesc);

            b.returnVoid();
        }


        // Add delegation for all abstract methods.  It is the responsibility of the implementor
        // to delegate the non-abstract methods

        for (Method method : interceptorType.getMethods()) {
            if (Modifier.isAbstract(method.getModifiers())) {
                MethodInfo mi = cf.addMethod(method);
                CodeBuilder b = new CodeBuilder(mi);

                // If we're asked to short circuit, we don't bother proxying.  This is useful
                // for creating a "visitor" -- that is, only implement a few "set" methods
                if (!doShortCircuit) {
                    b.loadThis();
                    b.loadField(PROXY, userTypeDesc);
                    for (int i = 0; i < method.getParameterTypes().length; i++) {
                        b.loadLocal(b.getParameter(i));
                    }
                    b.invoke(method);
                }

                if (void.class == method.getReturnType()) {
                    b.returnVoid();
                } else {
                    b.returnValue(TypeDesc.forClass(method.getReturnType()));
                }
            }
        }

        Class result = ci.defineClass(cf);
        return (Class<? extends T>) result;
    }

    /**
     * Create a new proxied storable instance which delegates to the given
     * proxies. All methods are fully delegated.
     *
     * @param storable to use as a proxy
     */
    public S create(Storable storable) {
        try {
            return mConstructor.newInstance(storable);
        }
        catch (InstantiationException e) {
            InternalError error = new InternalError();
            error.initCause(e);
            throw error;
        } catch (IllegalAccessException e) {
            InternalError error = new InternalError();
            error.initCause(e);
            throw error;
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            InternalError error = new InternalError();
            error.initCause(cause == null ? e : cause);
            throw error;
        }
    }
}
