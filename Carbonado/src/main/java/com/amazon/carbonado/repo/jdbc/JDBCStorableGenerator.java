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

package com.amazon.carbonado.repo.jdbc;

import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.cojen.classfile.ClassFile;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.Label;
import org.cojen.classfile.LocalVariable;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.Opcode;
import org.cojen.classfile.TypeDesc;
import org.cojen.util.ClassInjector;
import org.cojen.util.SoftValuedHashMap;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.OptimisticLockException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.lob.Lob;

import com.amazon.carbonado.info.StorablePropertyAdapter;

import com.amazon.carbonado.spi.CodeBuilderUtil;
import com.amazon.carbonado.spi.MasterFeature;
import com.amazon.carbonado.spi.MasterStorableGenerator;
import com.amazon.carbonado.spi.MasterSupport;
import com.amazon.carbonado.spi.StorableGenerator;
import com.amazon.carbonado.spi.TriggerSupport;

import static com.amazon.carbonado.spi.CommonMethodNames.*;

/**
 * Generates concrete implementations of {@link Storable} types for
 * {@link JDBCRepository}.
 *
 * @author Brian S O'Neill
 */
class JDBCStorableGenerator<S extends Storable> {
    // These method names end in "$" to prevent name collisions with any
    // inherited methods.
    private static final String EXTRACT_ALL_METHOD_NAME = "extractAll$";
    private static final String EXTRACT_DATA_METHOD_NAME = "extractData$";
    private static final String LOB_LOADER_FIELD_PREFIX = "lobLoader$";

    // Initial StringBuilder capactity for update statement.
    private static final int INITIAL_UPDATE_BUFFER_SIZE = 100;

    private static final Map<Class<?>, Class<? extends Storable>> cCache;

    static {
        cCache = new SoftValuedHashMap();
    }

    static <S extends Storable> Class<? extends S> getGeneratedClass(JDBCStorableInfo<S> info)
        throws SupportException
    {
        Class<S> type = info.getStorableType();
        synchronized (cCache) {
            Class<? extends S> generatedClass = (Class<? extends S>) cCache.get(type);
            if (generatedClass != null) {
                return generatedClass;
            }
            generatedClass = new JDBCStorableGenerator<S>(info).generateAndInjectClass();
            cCache.put(type, generatedClass);
            return generatedClass;
        }
    }

    private final Class<S> mStorableType;
    private final JDBCStorableInfo<S> mInfo;
    private final Map<String, ? extends JDBCStorableProperty<S>> mAllProperties;

    private final ClassLoader mParentClassLoader;
    private final ClassInjector mClassInjector;
    private final ClassFile mClassFile;

    private JDBCStorableGenerator(JDBCStorableInfo<S> info) throws SupportException {
        mStorableType = info.getStorableType();
        mInfo = info;
        mAllProperties = mInfo.getAllProperties();

        EnumSet<MasterFeature> features = EnumSet
            .of(MasterFeature.INSERT_SEQUENCES,
                MasterFeature.INSERT_TXN, MasterFeature.UPDATE_TXN);

        final Class<? extends S> abstractClass =
            MasterStorableGenerator.getAbstractClass(mStorableType, features);

        mParentClassLoader = abstractClass.getClassLoader();
        mClassInjector = ClassInjector.create(mStorableType.getName(), mParentClassLoader);

        mClassFile = new ClassFile(mClassInjector.getClassName(), abstractClass);
        mClassFile.markSynthetic();
        mClassFile.setSourceFile(JDBCStorableGenerator.class.getName());
        mClassFile.setTarget("1.5");
    }

    private Class<? extends S> generateAndInjectClass() {
        // We'll need these "inner classes" which serve as Lob loading
        // callbacks. Lob's need to be reloaded if the original transaction has
        // been committed.
        final Map<JDBCStorableProperty<S>, Class<?>> lobLoaderMap = generateLobLoaders();

        // Declare some types.
        final TypeDesc storageType = TypeDesc.forClass(Storage.class);
        final TypeDesc jdbcRepoType = TypeDesc.forClass(JDBCRepository.class);
        final TypeDesc jdbcSupportType = TypeDesc.forClass(JDBCSupport.class);
        final TypeDesc resultSetType = TypeDesc.forClass(ResultSet.class);
        final TypeDesc connectionType = TypeDesc.forClass(Connection.class);
        final TypeDesc preparedStatementType = TypeDesc.forClass(PreparedStatement.class);
        final TypeDesc lobArrayType = TypeDesc.forClass(Lob.class).toArrayType();
        final TypeDesc masterSupportType = TypeDesc.forClass(MasterSupport.class);
        final TypeDesc classType = TypeDesc.forClass(Class.class);

        if (lobLoaderMap.size() > 0) {
            // Add static initializer to save references to Lob
            // loaders. Otherwise, classes might get unloaded before they are
            // used for the first time.

            MethodInfo mi = mClassFile.addInitializer();
            CodeBuilder b = new CodeBuilder(mi);

            int i = 0;
            for (Class<?> loaderClass : lobLoaderMap.values()) {
                String fieldName = LOB_LOADER_FIELD_PREFIX + i;
                mClassFile.addField
                    (Modifiers.PRIVATE.toStatic(true).toFinal(true), fieldName, classType);
                b.loadConstant(TypeDesc.forClass(loaderClass));
                b.storeStaticField(fieldName, classType);
                i++;
            }

            b.returnVoid();
        }

        // Add constructor that accepts a JDBCSupport.
        {
            TypeDesc[] params = {jdbcSupportType};
            MethodInfo mi = mClassFile.addConstructor(Modifiers.PUBLIC, params);
            CodeBuilder b = new CodeBuilder(mi);
            b.loadThis();
            b.loadLocal(b.getParameter(0));
            b.checkCast(masterSupportType);
            b.invokeSuperConstructor(new TypeDesc[] {masterSupportType});
            b.returnVoid();
        }

        // Add constructor that accepts a JDBCSupport and a ResultSet row.
        {
            TypeDesc[] params = {jdbcSupportType, resultSetType, TypeDesc.INT};
            MethodInfo mi = mClassFile.addConstructor(Modifiers.PUBLIC, params);
            CodeBuilder b = new CodeBuilder(mi);
            b.loadThis();
            b.loadLocal(b.getParameter(0));
            b.checkCast(masterSupportType);
            b.invokeSuperConstructor(new TypeDesc[] {masterSupportType});

            // Call extractAll method to fill in properties.
            b.loadThis();
            b.loadLocal(b.getParameter(1));
            b.loadLocal(b.getParameter(2));
            b.invokePrivate(EXTRACT_ALL_METHOD_NAME, null,
                            new TypeDesc[] {resultSetType, TypeDesc.INT});

            // Indicate that object is clean by calling markAllPropertiesClean.
            b.loadThis();
            b.invokeVirtual(MARK_ALL_PROPERTIES_CLEAN, null, null);

            b.returnVoid();
        }

        // Add private method to extract all properties from a ResultSet row.
        defineExtractAllMethod(lobLoaderMap);
        // Add private method to extract non-pk properties from a ResultSet row.
        defineExtractDataMethod(lobLoaderMap);

        // For all unsupported properties, override get/set method to throw
        // UnsupportedOperationException.
        {
            for (JDBCStorableProperty<S> property : mAllProperties.values()) {
                if (property.isJoin() || property.isSupported()) {
                    continue;
                }
                String message = "Independent property \"" + property.getName() +
                    "\" is not supported by the SQL schema: ";
                message += mInfo.getTableName();
                CodeBuilder b = new CodeBuilder(mClassFile.addMethod(property.getReadMethod()));
                CodeBuilderUtil.throwException(b, UnsupportedOperationException.class, message);
                b = new CodeBuilder(mClassFile.addMethod(property.getWriteMethod()));
                CodeBuilderUtil.throwException(b, UnsupportedOperationException.class, message);
            }
        }

        // Add required protected doTryLoad method.
        {
            MethodInfo mi = mClassFile.addMethod
                (Modifiers.PROTECTED,
                 MasterStorableGenerator.DO_TRY_LOAD_MASTER_METHOD_NAME, TypeDesc.BOOLEAN, null);
            mi.addException(TypeDesc.forClass(FetchException.class));
            CodeBuilder b = new CodeBuilder(mi);

            LocalVariable repoVar = getJDBCRepository(b);
            Label tryBeforeCon = b.createLabel().setLocation();
            LocalVariable conVar = getConnection(b, repoVar);
            Label tryAfterCon = b.createLabel().setLocation();

            b.loadThis();
            b.loadLocal(repoVar);
            b.loadLocal(conVar);
            b.loadNull(); // No Lobs to update
            b.invokeVirtual(MasterStorableGenerator.DO_TRY_LOAD_MASTER_METHOD_NAME,
                            TypeDesc.BOOLEAN,
                            new TypeDesc[] {jdbcRepoType, connectionType, lobArrayType});
            LocalVariable resultVar = b.createLocalVariable("result", TypeDesc.BOOLEAN);
            b.storeLocal(resultVar);

            yieldConAndHandleException(b, repoVar, tryBeforeCon, conVar, tryAfterCon, false);

            b.loadLocal(resultVar);
            b.returnValue(TypeDesc.BOOLEAN);
        }

        // Now define doTryLoad(JDBCRepositry, Connection, Lob[]). The Lob array argument
        // is optional, and it indicates which (large) Lobs should be updated upon load.
        {
            MethodInfo mi = mClassFile.addMethod
                (Modifiers.PROTECTED,
                 MasterStorableGenerator.DO_TRY_LOAD_MASTER_METHOD_NAME, TypeDesc.BOOLEAN,
                 new TypeDesc[] {jdbcRepoType, connectionType, lobArrayType});
            mi.addException(TypeDesc.forClass(Exception.class));
            CodeBuilder b = new CodeBuilder(mi);

            StringBuilder selectBuilder = null;
            for (JDBCStorableProperty<S> property : mAllProperties.values()) {
                // Along with unsupported properties and joins, primary keys are not loaded.
                // This is because they are included in the where clause.
                if (!property.isSelectable() || property.isPrimaryKeyMember()) {
                    continue;
                }
                if (selectBuilder == null) {
                    selectBuilder = new StringBuilder();
                    selectBuilder.append("SELECT ");
                } else {
                    selectBuilder.append(',');
                }
                selectBuilder.append(property.getColumnName());
            }

            if (selectBuilder == null) {
                // All properties are pks. A select still needs to be
                // performed, but just discard the results. The select needs to
                // be performed in order to verify that a record exists, since
                // we need to return true or false.
                selectBuilder = new StringBuilder();
                selectBuilder.append("SELECT ");
                selectBuilder.append
                    (mInfo.getPrimaryKeyProperties().values().iterator().next().getColumnName());
            }

            selectBuilder.append(" FROM ");
            selectBuilder.append(mInfo.getQualifiedTableName());

            LocalVariable psVar = b.createLocalVariable("ps", preparedStatementType);

            Label tryAfterPs = buildWhereClauseAndPreparedStatement
                (b, selectBuilder, b.getParameter(1), psVar, b.getParameter(0), null);

            b.loadLocal(psVar);
            b.invokeInterface(preparedStatementType, "executeQuery", resultSetType, null);
            LocalVariable rsVar = b.createLocalVariable("rs", resultSetType);
            b.storeLocal(rsVar);
            Label tryAfterRs = b.createLabel().setLocation();

            // If no results, then return false. Otherwise, there must be
            // exactly one result.

            LocalVariable resultVar = b.createLocalVariable("result", TypeDesc.BOOLEAN);
            b.loadLocal(rsVar);
            b.invokeInterface(resultSetType, "next", TypeDesc.BOOLEAN, null);
            b.storeLocal(resultVar);
            b.loadLocal(resultVar);
            Label noResults = b.createLabel();
            b.ifZeroComparisonBranch(noResults, "==");

            b.loadThis();
            b.loadLocal(rsVar);
            b.loadConstant(1);
            b.loadLocal(b.getParameter(2)); // Pass Lobs to update
            b.invokePrivate(EXTRACT_DATA_METHOD_NAME, null,
                            new TypeDesc[] {resultSetType, TypeDesc.INT, lobArrayType});

            noResults.setLocation();

            closeResultSet(b, rsVar, tryAfterRs);
            closeStatement(b, psVar, tryAfterPs);

            b.loadLocal(resultVar);
            b.returnValue(TypeDesc.BOOLEAN);
        }

        // Unlike the other methods, doTryInsert is allowed to throw an
        // SQLException. Override insert and tryInsert to catch SQLException.
        // The tryInsert method must also decide if it is a unique constraint
        // exception and returns false instead. This design allows the original
        // SQLException to be passed with the UniqueConstraintException,
        // providing more context.

        // Override insert method.
        {
            MethodInfo mi = mClassFile.addMethod
                (Modifiers.PUBLIC, INSERT_METHOD_NAME, null, null);
            mi.addException(TypeDesc.forClass(PersistException.class));
            CodeBuilder b = new CodeBuilder(mi);

            Label tryStart = b.createLabel().setLocation();
            b.loadThis();
            b.invokeSuper(mClassFile.getSuperClassName(), INSERT_METHOD_NAME, null, null);
            Label tryEnd = b.createLabel().setLocation();
            b.returnVoid();

            b.exceptionHandler(tryStart, tryEnd, Exception.class.getName());
            pushJDBCRepository(b);
            // Swap exception object and JDBCRepository instance.
            b.swap();
            TypeDesc[] params = {TypeDesc.forClass(Throwable.class)};
            b.invokeVirtual(jdbcRepoType, "toPersistException",
                            TypeDesc.forClass(PersistException.class), params);
            b.throwObject();
        }

        // Override tryInsert method.
        {
            MethodInfo mi = mClassFile.addMethod
                (Modifiers.PUBLIC, TRY_INSERT_METHOD_NAME, TypeDesc.BOOLEAN, null);
            mi.addException(TypeDesc.forClass(PersistException.class));
            CodeBuilder b = new CodeBuilder(mi);

            Label tryStart = b.createLabel().setLocation();
            b.loadThis();
            b.invokeSuper(mClassFile.getSuperClassName(),
                          TRY_INSERT_METHOD_NAME, TypeDesc.BOOLEAN, null);
            Label innerTryEnd = b.createLabel().setLocation();
            b.returnValue(TypeDesc.BOOLEAN);

            b.exceptionHandler(tryStart, innerTryEnd, SQLException.class.getName());
            b.dup(); // dup the SQLException
            pushJDBCRepository(b);
            b.swap(); // swap the dup'ed SQLException to pass to method
            b.invokeVirtual(jdbcRepoType, "isUniqueConstraintError",
                            TypeDesc.BOOLEAN,
                            new TypeDesc[] {TypeDesc.forClass(SQLException.class)});
            Label notConstraint = b.createLabel();
            b.ifZeroComparisonBranch(notConstraint, "==");
            // Return false to indicate unique constraint violation.
            b.loadConstant(false);
            b.returnValue(TypeDesc.BOOLEAN);

            notConstraint.setLocation();
            // Re-throw SQLException, since it is not a unique constraint violation.
            b.throwObject();

            Label outerTryEnd = b.createLabel().setLocation();

            b.exceptionHandler(tryStart, outerTryEnd, Exception.class.getName());
            pushJDBCRepository(b);
            // Swap exception object and JDBCRepository instance.
            b.swap();
            TypeDesc[] params = {TypeDesc.forClass(Throwable.class)};
            b.invokeVirtual(jdbcRepoType, "toPersistException",
                            TypeDesc.forClass(PersistException.class), params);
            b.throwObject();
        }

        // Add required protected doTryInsert method.
        {
            MethodInfo mi = mClassFile.addMethod
                (Modifiers.PROTECTED,
                 MasterStorableGenerator.DO_TRY_INSERT_MASTER_METHOD_NAME, TypeDesc.BOOLEAN, null);
            mi.addException(TypeDesc.forClass(PersistException.class));
            CodeBuilder b = new CodeBuilder(mi);

            LocalVariable repoVar = getJDBCRepository(b);
            LocalVariable conVar = getConnection(b, repoVar);
            Label tryAfterCon = b.createLabel().setLocation();

            // Push connection in preparation for preparing a statement.
            b.loadLocal(conVar);

            // Only insert version property if DIRTY. Create two insert
            // statements, with and without the version property.
            StringBuilder sb = new StringBuilder();
            createInsertStatement(sb, false);
            String noVersion = sb.toString();

            sb.setLength(0);
            int versionPropNumber = createInsertStatement(sb, true);

            LocalVariable includeVersion = null;

            if (versionPropNumber < 0) {
                // No version property at all, so no need to determine which
                // statement to execute.
                b.loadConstant(noVersion);
            } else {
                includeVersion = b.createLocalVariable(null, TypeDesc.BOOLEAN);

                Label isDirty = b.createLabel();
                branchIfDirty(b, versionPropNumber, isDirty, true);

                // Version not dirty, so don't insert it. Assume database
                // creates an initial version instead.
                b.loadConstant(false);
                b.storeLocal(includeVersion);
                b.loadConstant(noVersion);
                Label cont = b.createLabel();
                b.branch(cont);

                isDirty.setLocation();
                // Including version property in statement.
                b.loadConstant(true);
                b.storeLocal(includeVersion);
                b.loadConstant(sb.toString());

                cont.setLocation();
            }

            // At this point, the stack contains a connection and a SQL
            // statement String.

            LocalVariable psVar = b.createLocalVariable("ps", preparedStatementType);
            b.invokeInterface(connectionType, "prepareStatement", preparedStatementType,
                              new TypeDesc[] {TypeDesc.STRING});
            b.storeLocal(psVar);

            Label tryAfterPs = b.createLabel().setLocation();

            // Now fill in parameters with property values.

            JDBCStorableProperty<S> versionProperty = null;

            // Gather all Lob properties to track if a post-insert update is required.
            Map<JDBCStorableProperty<S>, Integer> lobIndexMap = findLobs();
            LocalVariable lobArrayVar = null;
            if (lobIndexMap.size() != 0) {
                // Create array to track which Lobs are too large and need extra work.
                lobArrayVar = b.createLocalVariable(null, lobArrayType);
                b.loadConstant(lobIndexMap.size());
                b.newObject(lobArrayType);
                b.storeLocal(lobArrayVar);
            }

            int ordinal = 0;
            for (JDBCStorableProperty<S> property : mAllProperties.values()) {
                if (!property.isSelectable()) {
                    continue;
                }
                if (property.isVersion()) {
                    if (includeVersion != null) {
                        // Fill in version later, but check against boolean
                        // local variable to decide if it is was dirty.
                        versionProperty = property;
                    }
                    continue;
                }

                b.loadLocal(psVar);
                b.loadConstant(++ordinal);

                setPreparedStatementValue
                    (b, property, repoVar, null, lobArrayVar, lobIndexMap.get(property));
            }

            if (versionProperty != null) {
                // Fill in version property now, but only if was dirty.
                b.loadLocal(includeVersion);
                Label skipVersion = b.createLabel();
                b.ifZeroComparisonBranch(skipVersion, "==");

                b.loadLocal(psVar);
                b.loadConstant(++ordinal);
                setPreparedStatementValue(b, versionProperty, repoVar, null, null, null);

                skipVersion.setLocation();
            }

            // Execute the statement.
            b.loadLocal(psVar);
            b.invokeInterface(preparedStatementType, "executeUpdate", TypeDesc.INT, null);
            b.pop();
            closeStatement(b, psVar, tryAfterPs);

            // Immediately reload object, to ensure that any database supplied
            // default values are properly retrieved. Since INSERT_TXN is
            // enabled, superclass ensures that transaction is still in
            // progress at this point.

            b.loadThis();
            b.loadLocal(repoVar);
            b.loadLocal(conVar);
            if (lobArrayVar == null) {
                b.loadNull();
            } else {
                b.loadLocal(lobArrayVar);
            }
            b.invokeVirtual(MasterStorableGenerator.DO_TRY_LOAD_MASTER_METHOD_NAME,
                            TypeDesc.BOOLEAN,
                            new TypeDesc[] {jdbcRepoType, connectionType, lobArrayType});
            b.pop();

            // Note: yieldConAndHandleException is not called, allowing any
            // SQLException to be thrown. The insert or tryInsert methods must handle it.
            yieldCon(b, repoVar, conVar, tryAfterCon);

            b.loadConstant(true);
            b.returnValue(TypeDesc.BOOLEAN);
        }

        // Add required protected doTryUpdate method.
        {
            MethodInfo mi = mClassFile.addMethod
                (Modifiers.PROTECTED,
                 MasterStorableGenerator.DO_TRY_UPDATE_MASTER_METHOD_NAME,
                 TypeDesc.BOOLEAN, null);
            mi.addException(TypeDesc.forClass(PersistException.class));

            CodeBuilder b = new CodeBuilder(mi);

            // Only update properties with state DIRTY. Therefore, update
            // statement is always dynamic.

            LocalVariable repoVar = getJDBCRepository(b);
            Label tryBeforeCon = b.createLabel().setLocation();
            LocalVariable conVar = getConnection(b, repoVar);
            Label tryAfterCon = b.createLabel().setLocation();

            // Load connection in preparation for creating statement.
            b.loadLocal(conVar);

            TypeDesc stringBuilderType = TypeDesc.forClass(StringBuilder.class);
            b.newObject(stringBuilderType);
            b.dup();
            b.loadConstant(INITIAL_UPDATE_BUFFER_SIZE);
            b.invokeConstructor(stringBuilderType, new TypeDesc[] {TypeDesc.INT});

            // Methods on StringBuilder.
            final Method appendStringMethod;
            final Method appendCharMethod;
            final Method toStringMethod;
            try {
                appendStringMethod = StringBuilder.class.getMethod("append", String.class);
                appendCharMethod = StringBuilder.class.getMethod("append", char.class);
                toStringMethod = StringBuilder.class.getMethod("toString", (Class[]) null);
            } catch (NoSuchMethodException e) {
                throw new UndeclaredThrowableException(e);
            }

            {
                StringBuilder sqlBuilder = new StringBuilder();
                sqlBuilder.append("UPDATE ");
                sqlBuilder.append(mInfo.getQualifiedTableName());
                sqlBuilder.append(" SET ");

                b.loadConstant(sqlBuilder.toString());
                b.invoke(appendStringMethod); // method leaves StringBuilder on stack
            }

            // Iterate over the properties, appending a set parameter for each
            // that is dirty.

            LocalVariable countVar = b.createLocalVariable("count", TypeDesc.INT);
            b.loadConstant(0);
            b.storeLocal(countVar);

            int propNumber = -1;
            for (JDBCStorableProperty property : mAllProperties.values()) {
                propNumber++;

                if (property.isSelectable() && !property.isPrimaryKeyMember()) {
                    if (property.isVersion()) {
                        // TODO: Support option where version property is
                        // updated on the Carbonado side rather than relying on
                        // SQL trigger.
                        continue;
                    }

                    Label isNotDirty = b.createLabel();
                    branchIfDirty(b, propNumber, isNotDirty, false);

                    b.loadLocal(countVar);
                    Label isZero = b.createLabel();
                    b.ifZeroComparisonBranch(isZero, "==");
                    b.loadConstant(',');
                    b.invoke(appendCharMethod);

                    isZero.setLocation();
                    b.loadConstant(property.getColumnName());
                    b.invoke(appendStringMethod);
                    b.loadConstant("=?");
                    b.invoke(appendStringMethod);

                    b.integerIncrement(countVar, 1);

                    isNotDirty.setLocation();
                }
            }

            Collection<JDBCStorableProperty<S>> whereProperties =
                mInfo.getPrimaryKeyProperties().values();

            JDBCStorableProperty<S> versionProperty = mInfo.getVersionProperty();
            if (versionProperty != null && versionProperty.isSupported()) {
                // Include version property in WHERE clause to support optimistic locking.
                List<JDBCStorableProperty<S>> list =
                    new ArrayList<JDBCStorableProperty<S>>(whereProperties);
                list.add(versionProperty);
                whereProperties = list;
            }

            // If no dirty properties, a valid update statement must still be
            // created. Just update the first "where" property to itself.
            {
                b.loadLocal(countVar);
                Label notZero = b.createLabel();
                b.ifZeroComparisonBranch(notZero, "!=");

                b.loadConstant(whereProperties.iterator().next().getColumnName());
                b.invoke(appendStringMethod);
                b.loadConstant("=?");
                b.invoke(appendStringMethod);

                notZero.setLocation();
            }

            b.loadConstant(" WHERE ");
            b.invoke(appendStringMethod);

            int ordinal = 0;
            for (JDBCStorableProperty<S> property : whereProperties) {
                if (ordinal > 0) {
                    b.loadConstant(" AND ");
                    b.invoke(appendStringMethod);
                }
                b.loadConstant(property.getColumnName());
                b.invoke(appendStringMethod);
                if (property.isNullable()) {
                    // FIXME
                    throw new UnsupportedOperationException();
                } else {
                    b.loadConstant("=?");
                    b.invoke(appendStringMethod);
                }
                ordinal++;
            }

            // Convert StringBuilder value to a String.
            b.invoke(toStringMethod);

            // At this point, the stack contains a connection and a SQL
            // statement String.

            LocalVariable psVar = b.createLocalVariable("ps", preparedStatementType);
            b.invokeInterface(connectionType, "prepareStatement", preparedStatementType,
                              new TypeDesc[] {TypeDesc.STRING});
            b.storeLocal(psVar);
            Label tryAfterPs = b.createLabel().setLocation();

            // Walk through dirty properties again, setting values on statement.

            LocalVariable indexVar = b.createLocalVariable("index", TypeDesc.INT);
            // First prepared statement index is always one, so says JDBC.
            b.loadConstant(1);
            b.storeLocal(indexVar);

            // Gather all Lob properties to track if a post-update update is required.
            Map<JDBCStorableProperty<S>, Integer> lobIndexMap = findLobs();
            LocalVariable lobArrayVar = null;
            if (lobIndexMap.size() != 0) {
                // Create array to track which Lobs are too large and need extra work.
                lobArrayVar = b.createLocalVariable(null, lobArrayType);
                b.loadConstant(lobIndexMap.size());
                b.newObject(lobArrayType);
                b.storeLocal(lobArrayVar);
            }

            // If no dirty properties, fill in extra property from before.
            {
                b.loadLocal(countVar);
                Label notZero = b.createLabel();
                b.ifZeroComparisonBranch(notZero, "!=");

                JDBCStorableProperty property = whereProperties.iterator().next();

                b.loadLocal(psVar);
                b.loadLocal(indexVar);
                setPreparedStatementValue
                    (b, property, repoVar, null, lobArrayVar, lobIndexMap.get(property));

                b.integerIncrement(indexVar, 1);

                notZero.setLocation();
            }

            propNumber = -1;
            for (JDBCStorableProperty property : mAllProperties.values()) {
                propNumber++;

                if (property.isSelectable() && !property.isPrimaryKeyMember()) {
                    if (property.isVersion()) {
                        // TODO: Support option where version property is
                        // updated on the Carbonado side rather than relying on
                        // SQL trigger. Just add one to the value.
                        continue;
                    }

                    Label isNotDirty = b.createLabel();
                    branchIfDirty(b, propNumber, isNotDirty, false);

                    b.loadLocal(psVar);
                    b.loadLocal(indexVar);
                    setPreparedStatementValue
                        (b, property, repoVar, null, lobArrayVar, lobIndexMap.get(property));

                    b.integerIncrement(indexVar, 1);

                    isNotDirty.setLocation();
                }
            }

            // Walk through where clause properties again, setting values on
            // statement.

            for (JDBCStorableProperty<S> property : whereProperties) {
                if (property.isNullable()) {
                    // FIXME
                    throw new UnsupportedOperationException();
                } else {
                    b.loadLocal(psVar);
                    b.loadLocal(indexVar);
                    setPreparedStatementValue(b, property, repoVar, null, null, null);
                }

                b.integerIncrement(indexVar, 1);
            }

            // Execute the update statement.

            b.loadLocal(psVar);
            LocalVariable updateCount = b.createLocalVariable("updateCount", TypeDesc.INT);
            b.invokeInterface(preparedStatementType, "executeUpdate", TypeDesc.INT, null);
            b.storeLocal(updateCount);

            closeStatement(b, psVar, tryAfterPs);

            Label doReload = b.createLabel();
            Label skipReload = b.createLabel();

            if (versionProperty == null) {
                b.loadLocal(updateCount);
                b.ifZeroComparisonBranch(skipReload, "==");
            } else {
                // If update count is zero, either the record was deleted or
                // the version doesn't match. To distinguish these two cases,
                // select record version. If not found, return
                // false. Otherwise, throw OptimisticLockException.

                b.loadLocal(updateCount);
                b.ifZeroComparisonBranch(doReload, "!=");

                StringBuilder selectBuilder = new StringBuilder();
                selectBuilder.append("SELECT ");
                selectBuilder.append(versionProperty.getColumnName());
                selectBuilder.append(" FROM ");
                selectBuilder.append(mInfo.getQualifiedTableName());

                LocalVariable countPsVar = b.createLocalVariable("ps", preparedStatementType);

                Label tryAfterCountPs = buildWhereClauseAndPreparedStatement
                    (b, selectBuilder, conVar, countPsVar, null, null);

                b.loadLocal(countPsVar);
                b.invokeInterface(preparedStatementType, "executeQuery", resultSetType, null);
                LocalVariable rsVar = b.createLocalVariable("rs", resultSetType);
                b.storeLocal(rsVar);
                Label tryAfterRs = b.createLabel().setLocation();

                b.loadLocal(rsVar);
                b.invokeInterface(resultSetType, "next", TypeDesc.BOOLEAN, null);
                // Record missing, return false.
                b.ifZeroComparisonBranch(skipReload, "==");

                b.loadLocal(rsVar);
                b.loadConstant(1); // column 1
                b.invokeInterface(resultSetType, "getLong",
                                  TypeDesc.LONG, new TypeDesc[] {TypeDesc.INT});
                LocalVariable actualVersion = b.createLocalVariable(null, TypeDesc.LONG);
                b.storeLocal(actualVersion);

                closeResultSet(b, rsVar, tryAfterRs);
                closeStatement(b, countPsVar, tryAfterCountPs);

                // Throw exception.
                {
                    TypeDesc desc = TypeDesc.forClass(OptimisticLockException.class);
                    b.newObject(desc);
                    b.dup();
                    b.loadThis();
                    // Pass expected version number for exception message.
                    TypeDesc propertyType = TypeDesc.forClass(versionProperty.getType());
                    b.loadField(versionProperty.getName(), propertyType);
                    b.convert(propertyType, TypeDesc.LONG.toObjectType());
                    b.loadLocal(actualVersion);
                    b.convert(TypeDesc.LONG, TypeDesc.LONG.toObjectType());
                    b.invokeConstructor(desc, new TypeDesc[] {TypeDesc.OBJECT, TypeDesc.OBJECT});
                    b.throwObject();
                }
            }

            // Immediately reload object, to ensure that any database supplied
            // default values are properly retrieved. Since UPDATE_TXN is
            // enabled, superclass ensures that transaction is still in
            // progress at this point.

            doReload.setLocation();
            b.loadThis();
            b.loadLocal(repoVar);
            b.loadLocal(conVar);
            if (lobArrayVar == null) {
                b.loadNull();
            } else {
                b.loadLocal(lobArrayVar);
            }
            b.invokeVirtual(MasterStorableGenerator.DO_TRY_LOAD_MASTER_METHOD_NAME,
                            TypeDesc.BOOLEAN,
                            new TypeDesc[] {jdbcRepoType, connectionType, lobArrayType});
            // Even though a boolean is returned, the actual value for true and
            // false is an int, 1 or 0.
            b.storeLocal(updateCount);

            skipReload.setLocation();

            yieldConAndHandleException(b, repoVar, tryBeforeCon, conVar, tryAfterCon, true);

            b.loadLocal(updateCount);
            b.returnValue(TypeDesc.BOOLEAN);
        }

        // Add required protected doTryDelete method.
        {
            MethodInfo mi = mClassFile.addMethod
                (Modifiers.PROTECTED,
                 MasterStorableGenerator.DO_TRY_DELETE_MASTER_METHOD_NAME, TypeDesc.BOOLEAN, null);
            mi.addException(TypeDesc.forClass(PersistException.class));
            CodeBuilder b = new CodeBuilder(mi);

            StringBuilder deleteBuilder = new StringBuilder();
            deleteBuilder.append("DELETE FROM ");
            deleteBuilder.append(mInfo.getQualifiedTableName());

            LocalVariable repoVar = getJDBCRepository(b);
            Label tryBeforeCon = b.createLabel().setLocation();
            LocalVariable conVar = getConnection(b, repoVar);
            Label tryAfterCon = b.createLabel().setLocation();

            LocalVariable psVar = b.createLocalVariable("ps", preparedStatementType);

            Label tryAfterPs = buildWhereClauseAndPreparedStatement
                (b, deleteBuilder, conVar, psVar, null, null);

            b.loadLocal(psVar);
            b.invokeInterface(preparedStatementType, "executeUpdate", TypeDesc.INT, null);

            // Return false if count is zero, true otherwise. Just return the
            // int as if it were boolean.

            LocalVariable resultVar = b.createLocalVariable("result", TypeDesc.INT);
            b.storeLocal(resultVar);

            closeStatement(b, psVar, tryAfterPs);
            yieldConAndHandleException(b, repoVar, tryBeforeCon, conVar, tryAfterCon, true);

            b.loadLocal(resultVar);
            b.returnValue(TypeDesc.BOOLEAN);
        }

        Class<? extends S> generatedClass = mClassInjector.defineClass(mClassFile);

        // Touch lobLoaderMap to ensure reference to these classes are kept
        // until after storable class is generated. Otherwise, these classes
        // might get unloaded.
        lobLoaderMap.size();

        return generatedClass;
    }

    /**
     * Finds all Lob properties and maps them to a zero-based index. This
     * information is used to update large Lobs after an insert or update.
     */
    private Map<JDBCStorableProperty<S>, Integer>findLobs() {
        Map<JDBCStorableProperty<S>, Integer> lobIndexMap =
            new IdentityHashMap<JDBCStorableProperty<S>, Integer>();

        int lobIndex = 0;

        for (JDBCStorableProperty<S> property : mAllProperties.values()) {
            if (!property.isSelectable() || property.isVersion()) {
                continue;
            }

            Class psClass = property.getPreparedStatementSetMethod().getParameterTypes()[1];

            if (Lob.class.isAssignableFrom(property.getType()) ||
                java.sql.Blob.class.isAssignableFrom(psClass) ||
                java.sql.Clob.class.isAssignableFrom(psClass)) {

                lobIndexMap.put(property, lobIndex++);
            }
        }

        return lobIndexMap;
    }

    /**
     * Generates code to get the JDBCRepository instance and store it in a
     * local variable.
     */
    private LocalVariable getJDBCRepository(CodeBuilder b) {
        pushJDBCRepository(b);
        LocalVariable repoVar =
            b.createLocalVariable("repo", TypeDesc.forClass(JDBCRepository.class));
        b.storeLocal(repoVar);
        return repoVar;
    }

    /**
     * Generates code to push the JDBCRepository instance on the stack.
     */
    private void pushJDBCRepository(CodeBuilder b) {
        pushJDBCSupport(b);
        b.invokeInterface(TypeDesc.forClass(JDBCSupport.class), "getJDBCRepository",
                          TypeDesc.forClass(JDBCRepository.class), null);
    }

    /**
     * Generates code to push the JDBCSupport instance on the stack.
     */
    private void pushJDBCSupport(CodeBuilder b) {
        b.loadThis();
        b.loadField(StorableGenerator.SUPPORT_FIELD_NAME, TypeDesc.forClass(TriggerSupport.class));
        b.checkCast(TypeDesc.forClass(JDBCSupport.class));
    }

    /**
     * Generates code to get connection from JDBCRepository and store it in a local variable.
     *
     * @param repoVar reference to JDBCRepository
     */
    private LocalVariable getConnection(CodeBuilder b, LocalVariable repoVar) {
        b.loadLocal(repoVar);
        b.invokeVirtual(TypeDesc.forClass(JDBCRepository.class),
                        "getConnection", TypeDesc.forClass(Connection.class), null);
        LocalVariable conVar = b.createLocalVariable("con", TypeDesc.forClass(Connection.class));
        b.storeLocal(conVar);
        return conVar;
    }

    /**
     * Generates code which emulates this:
     *
     *     // May throw FetchException
     *     JDBCRepository.yieldConnection(con);
     *
     * @param repoVar required reference to JDBCRepository
     * @param conVar optional connection to yield
     */
    private void yieldConnection(CodeBuilder b, LocalVariable repoVar, LocalVariable conVar) {
        if (conVar != null) {
            b.loadLocal(repoVar);
            b.loadLocal(conVar);
            b.invokeVirtual(TypeDesc.forClass(JDBCRepository.class),
                            "yieldConnection", null,
                            new TypeDesc[] {TypeDesc.forClass(Connection.class)});
        }
    }

    /**
     * Generates code that finishes the given SQL statement by appending a
     * WHERE clause. Prepared statement is then created and all parameters are
     * filled in.
     *
     * @param sqlBuilder contains SQL statement right before the WHERE clause
     * @param conVar local variable referencing connection
     * @param psVar declared local variable which will receive PreparedStatement
     * @param jdbcRepoVar when non-null, check transaction if SELECT should be FOR UPDATE
     * @param instanceVar when null, assume properties are contained in
     * "this". Otherwise, invoke property access methods on storable referenced
     * in var.
     * @return label right after prepared statement was created, which is to be
     * used as the start of a try block that ensures the prepared statement is
     * closed.
     */
    private Label buildWhereClauseAndPreparedStatement
        (CodeBuilder b,
         StringBuilder sqlBuilder,
         LocalVariable conVar,
         LocalVariable psVar,
         LocalVariable jdbcRepoVar,
         LocalVariable instanceVar)
    {
        final TypeDesc superType = TypeDesc.forClass(mClassFile.getSuperClassName());
        final Iterable<? extends JDBCStorableProperty<?>> properties =
            mInfo.getPrimaryKeyProperties().values();

        sqlBuilder.append(" WHERE ");

        List<JDBCStorableProperty> nullableProperties = new ArrayList<JDBCStorableProperty>();
        int ordinal = 0;
        for (JDBCStorableProperty property : properties) {
            if (!property.isSelectable()) {
                continue;
            }
            if (property.isNullable()) {
                // Nullable properties need to alter the SQL where clause
                // syntax at runtime, taking the forms "=?" or "IS NULL".
                nullableProperties.add(property);
                continue;
            }
            if (ordinal > 0) {
                sqlBuilder.append(" AND ");
            }
            sqlBuilder.append(property.getColumnName());
            sqlBuilder.append("=?");
            ordinal++;
        }

        // Push connection in preparation for preparing a statement.
        b.loadLocal(conVar);

        if (nullableProperties.size() == 0) {
            b.loadConstant(sqlBuilder.toString());

            // Determine at runtime if SELECT should be " FOR UPDATE".
            if (jdbcRepoVar != null) {
                b.loadLocal(jdbcRepoVar);
                b.invokeVirtual
                    (jdbcRepoVar.getType(), "isTransactionForUpdate", TypeDesc.BOOLEAN, null);
                Label notForUpdate = b.createLabel();
                b.ifZeroComparisonBranch(notForUpdate, "==");

                b.loadConstant(" FOR UPDATE");
                b.invokeVirtual(TypeDesc.STRING, "concat",
                                TypeDesc.STRING, new TypeDesc[] {TypeDesc.STRING});

                notForUpdate.setLocation();
            }
        } else {
            // Finish select statement at runtime, since we don't know if the
            // properties are null or not.
            if (ordinal > 0) {
                sqlBuilder.append(" AND ");
            }

            // Make runtime buffer capacity large enough to hold all "IS NULL" phrases.
            int capacity = sqlBuilder.length() + 7 * nullableProperties.size();
            if (nullableProperties.size() > 1) {
                // Account for all the appended " AND " phrases.
                capacity += 5 * (nullableProperties.size() - 1);
            }
            for (JDBCStorableProperty property : nullableProperties) {
                // Account for property names.
                capacity += property.getColumnName().length();
            }

            TypeDesc stringBuilderType = TypeDesc.forClass(StringBuilder.class);
            b.newObject(stringBuilderType);
            b.dup();
            b.loadConstant(capacity);
            b.invokeConstructor(stringBuilderType, new TypeDesc[] {TypeDesc.INT});

            // Methods on StringBuilder.
            final Method appendStringMethod;
            final Method toStringMethod;
            try {
                appendStringMethod = StringBuilder.class.getMethod("append", String.class);
                toStringMethod = StringBuilder.class.getMethod("toString", (Class[]) null);
            } catch (NoSuchMethodException e) {
                throw new UndeclaredThrowableException(e);
            }

            b.loadConstant(sqlBuilder.toString());
            b.invoke(appendStringMethod); // method leaves StringBuilder on stack

            ordinal = 0;
            for (JDBCStorableProperty property : nullableProperties) {
                if (ordinal > 0) {
                    b.loadConstant(" AND ");
                    b.invoke(appendStringMethod);
                }

                b.loadConstant(property.getColumnName());
                b.invoke(appendStringMethod);

                b.loadThis();

                final TypeDesc propertyType = TypeDesc.forClass(property.getType());
                b.loadField(superType, property.getName(), propertyType);

                Label notNull = b.createLabel();
                b.ifNullBranch(notNull, false);
                b.loadConstant("IS NULL");
                b.invoke(appendStringMethod);
                Label next = b.createLabel();
                b.branch(next);

                notNull.setLocation();
                b.loadConstant("=?");
                b.invoke(appendStringMethod);

                next.setLocation();
                ordinal++;
            }

            // Determine at runtime if SELECT should be " FOR UPDATE".
            if (jdbcRepoVar != null) {
                b.loadLocal(jdbcRepoVar);
                b.invokeVirtual
                    (jdbcRepoVar.getType(), "isTransactionForUpdate", TypeDesc.BOOLEAN, null);
                Label notForUpdate = b.createLabel();
                b.ifZeroComparisonBranch(notForUpdate, "==");

                b.loadConstant(" FOR UPDATE");
                b.invoke(appendStringMethod);

                notForUpdate.setLocation();
            }

            // Convert StringBuilder to String.
            b.invoke(toStringMethod);
        }

        // At this point, the stack contains a connection and a SQL statement String.

        final TypeDesc connectionType = TypeDesc.forClass(Connection.class);
        final TypeDesc preparedStatementType = TypeDesc.forClass(PreparedStatement.class);

        b.invokeInterface(connectionType, "prepareStatement", preparedStatementType,
                          new TypeDesc[] {TypeDesc.STRING});
        b.storeLocal(psVar);
        Label tryAfterPs = b.createLabel().setLocation();

        // Now set where clause parameters.
        ordinal = 0;
        for (JDBCStorableProperty property : properties) {
            if (!property.isSelectable()) {
                continue;
            }

            Label skipProperty = b.createLabel();

            final TypeDesc propertyType = TypeDesc.forClass(property.getType());

            if (!property.isNullable()) {
                b.loadLocal(psVar);
                b.loadConstant(++ordinal);
            } else {
                // Nullable properties are dynamically added to where clause,
                // and are at the end of the prepared statement. If value is
                // null, then skip to the next property, since the statement
                // was appended earlier with "IS NULL".
                b.loadThis();
                b.loadField(superType, property.getName(), propertyType);
                b.ifNullBranch(skipProperty, true);
            }

            setPreparedStatementValue(b, property, null, instanceVar, null, null);

            skipProperty.setLocation();
        }

        return tryAfterPs;
    }

    /**
     * Generates code to call a PreparedStatement.setXxx(int, Xxx) method, with
     * the value of the given property. Assumes that PreparedStatement and int
     * index are on the stack.
     *
     * If the property is a Lob, then pass in the optional lobTooLargeVar to
     * track if it was too large to insert/update. The type of lobTooLargeVar
     * must be the carbonado lob type. At runtime, if the variable's value is
     * not null, then lob was too large to insert. The value of the variable is
     * the original lob. An update statement needs to be issued after the load
     * to insert/update the large value.
     *
     * @param instanceVar when null, assume properties are contained in
     * "this". Otherwise, invoke property access methods on storable referenced
     * in var.
     * @param lobArrayVar optional, used for lob properties
     * @param lobIndex optional, used for lob properties
     */
    private void setPreparedStatementValue
        (CodeBuilder b, JDBCStorableProperty<?> property, LocalVariable repoVar,
         LocalVariable instanceVar,
         LocalVariable lobArrayVar, Integer lobIndex)
    {
        if (instanceVar == null) {
            b.loadThis();
        } else {
            b.loadLocal(instanceVar);
        }

        Class psClass = property.getPreparedStatementSetMethod().getParameterTypes()[1];
        TypeDesc psType = TypeDesc.forClass(psClass);
        TypeDesc propertyType = TypeDesc.forClass(property.getType());

        StorablePropertyAdapter adapter = property.getAppliedAdapter();
        TypeDesc fromType;
        if (adapter == null) {
            // Get protected field directly, since no adapter.
            if (instanceVar == null) {
                b.loadField(property.getName(), propertyType);
            } else {
                b.loadField(instanceVar.getType(), property.getName(), propertyType);
            }
            fromType = propertyType;
        } else {
            Class toClass = psClass;
            if (java.sql.Blob.class.isAssignableFrom(toClass)) {
                toClass = com.amazon.carbonado.lob.Blob.class;
            } else if (java.sql.Clob.class.isAssignableFrom(toClass)) {
                toClass = com.amazon.carbonado.lob.Clob.class;
            }
            Method adaptMethod = adapter.findAdaptMethod(property.getType(), toClass);
            TypeDesc adaptType = TypeDesc.forClass(adaptMethod.getReturnType());
            // Invoke special inherited protected method that gets the field
            // and invokes the adapter. Method was generated by
            // StorableGenerator.
            String methodName = property.getReadMethodName() + '$';
            if (instanceVar == null) {
                b.invokeVirtual(methodName, adaptType, null);
            } else {
                b.invokeVirtual (instanceVar.getType(), methodName, adaptType, null);
            }
            fromType = adaptType;
        }

        Label done = b.createLabel();

        if (!fromType.isPrimitive()) {
            // Handle case where property value is null.
            b.dup();
            Label notNull = b.createLabel();
            b.ifNullBranch(notNull, false);
            // Invoke setNull method instead.
            b.pop(); // discard duplicate null.
            b.loadConstant(property.getDataType());
            b.invokeInterface(TypeDesc.forClass(PreparedStatement.class), "setNull",
                              null, new TypeDesc[] {TypeDesc.INT, TypeDesc.INT});
            b.branch(done);
            notNull.setLocation();
        }

        if (Lob.class.isAssignableFrom(fromType.toClass())) {
            // Run special conversion.

            LocalVariable lobVar = b.createLocalVariable(null, fromType);
            b.storeLocal(lobVar);
            LocalVariable columnVar = b.createLocalVariable(null, TypeDesc.INT);
            b.storeLocal(columnVar);
            LocalVariable psVar = b.createLocalVariable
                ("ps", TypeDesc.forClass(PreparedStatement.class));
            b.storeLocal(psVar);

            if (lobArrayVar != null && lobIndex != null) {
                // Prepare for update result. If too large, then array entry is not null.
                b.loadLocal(lobArrayVar);
                b.loadConstant(lobIndex);
            }

            pushJDBCSupport(b);
            b.loadLocal(psVar);
            b.loadLocal(columnVar);
            b.loadLocal(lobVar);

            // Stack looks like this: JDBCSupport, PreparedStatement, int (column), Lob

            Method setValueMethod;
            try {
                String name = fromType.toClass().getName();
                name = "set" + name.substring(name.lastIndexOf('.') + 1) + "Value";
                setValueMethod = JDBCSupport.class.getMethod
                    (name, PreparedStatement.class, int.class, fromType.toClass());
            } catch (NoSuchMethodException e) {
                throw new UndeclaredThrowableException(e);
            }

            b.invoke(setValueMethod);

            if (lobArrayVar == null || lobIndex == null) {
                b.pop();
            } else {
                b.storeToArray(TypeDesc.OBJECT);
            }
        } else {
            b.convert(fromType, psType);
            b.invoke(property.getPreparedStatementSetMethod());
        }

        done.setLocation();
    }

    /**
     * Generates code which emulates this:
     *
     * ...
     * } finally {
     *     JDBCRepository.yieldConnection(con);
     * }
     *
     * @param repoVar required reference to JDBCRepository
     * @param conVar optional connection variable
     * @param tryAfterCon label right after connection acquisition
     */
    private void yieldCon
        (CodeBuilder b,
         LocalVariable repoVar,
         LocalVariable conVar,
         Label tryAfterCon)
    {
        Label endFinallyLabel = b.createLabel().setLocation();
        Label contLabel = b.createLabel();

        yieldConnection(b, repoVar, conVar);
        b.branch(contLabel);

        b.exceptionHandler(tryAfterCon, endFinallyLabel, null);
        yieldConnection(b, repoVar, conVar);
        b.throwObject();

        contLabel.setLocation();
    }

    /**
     * Generates code which emulates this:
     *
     * ...
     *     } finally {
     *         JDBCRepository.yieldConnection(con);
     *     }
     * } catch (Exception e) {
     *     throw JDBCRepository.toFetchException(e);
     * }
     *
     * @param repoVar required reference to JDBCRepository
     * @param txnVar optional transaction variable to commit/exit
     * @param tryBeforeCon label right before connection acquisition
     * @param conVar optional connection variable
     * @param tryAfterCon label right after connection acquisition
     */
    private void yieldConAndHandleException
        (CodeBuilder b,
         LocalVariable repoVar,
         Label tryBeforeCon, LocalVariable conVar, Label tryAfterCon,
         boolean forPersist)
    {
        Label endFinallyLabel = b.createLabel().setLocation();
        Label contLabel = b.createLabel();

        yieldConnection(b, repoVar, conVar);
        b.branch(contLabel);

        b.exceptionHandler(tryAfterCon, endFinallyLabel, null);
        yieldConnection(b, repoVar, conVar);
        b.throwObject();

        b.exceptionHandler
            (tryBeforeCon, b.createLabel().setLocation(), Exception.class.getName());
        b.loadLocal(repoVar);
        // Swap exception object and JDBCRepository instance.
        b.swap();
        TypeDesc[] params = {TypeDesc.forClass(Throwable.class)};
        if (forPersist) {
            b.invokeVirtual(TypeDesc.forClass(JDBCRepository.class), "toPersistException",
                            TypeDesc.forClass(PersistException.class), params);
        } else {
            b.invokeVirtual(TypeDesc.forClass(JDBCRepository.class), "toFetchException",
                            TypeDesc.forClass(FetchException.class), params);
        }
        b.throwObject();

        contLabel.setLocation();
    }

    /**
     * Generates code which emulates this:
     *
     * ...
     * } finally {
     *     statement.close();
     * }
     *
     * @param statementVar Statement variable
     * @param tryAfterStatement label right after Statement acquisition
     */
    private void closeStatement
        (CodeBuilder b, LocalVariable statementVar, Label tryAfterStatement)
    {
        Label contLabel = b.createLabel();
        Label endFinallyLabel = b.createLabel().setLocation();

        b.loadLocal(statementVar);
        b.invokeInterface(TypeDesc.forClass(Statement.class), "close", null, null);
        b.branch(contLabel);

        b.exceptionHandler(tryAfterStatement, endFinallyLabel, null);
        b.loadLocal(statementVar);
        b.invokeInterface(TypeDesc.forClass(Statement.class), "close", null, null);
        b.throwObject();

        contLabel.setLocation();
    }

    /**
     * Generates code which emulates this:
     *
     * ...
     * } finally {
     *     rs.close();
     * }
     *
     * @param rsVar ResultSet variable
     * @param tryAfterRs label right after ResultSet acquisition
     */
    private void closeResultSet
        (CodeBuilder b, LocalVariable rsVar, Label tryAfterRs)
    {
        Label contLabel = b.createLabel();
        Label endFinallyLabel = b.createLabel().setLocation();

        b.loadLocal(rsVar);
        b.invokeInterface(TypeDesc.forClass(ResultSet.class), "close", null, null);
        b.branch(contLabel);

        b.exceptionHandler(tryAfterRs, endFinallyLabel, null);
        b.loadLocal(rsVar);
        b.invokeInterface(TypeDesc.forClass(ResultSet.class), "close", null, null);
        b.throwObject();

        contLabel.setLocation();
    }

    /**
     * Generates code to branch if a property is dirty.
     *
     * @param propNumber property number from all properties map
     * @param target branch target
     * @param when true, branch if dirty; when false, branch when not dirty
     */
    private void branchIfDirty(CodeBuilder b, int propNumber,
                               Label target, boolean branchIfDirty)
    {
        String stateFieldName = StorableGenerator.PROPERTY_STATE_FIELD_NAME + (propNumber >> 4);
        b.loadThis();
        b.loadField(stateFieldName, TypeDesc.INT);

        int shift = (propNumber & 0xf) * 2;
        b.loadConstant(StorableGenerator.PROPERTY_STATE_MASK << shift);
        b.math(Opcode.IAND);
        b.loadConstant(StorableGenerator.PROPERTY_STATE_DIRTY << shift);

        b.ifComparisonBranch(target, branchIfDirty ? "==" : "!=");
    }

    private void defineExtractAllMethod(Map<JDBCStorableProperty<S>, Class<?>> lobLoaderMap) {
        MethodInfo mi = mClassFile.addMethod
            (Modifiers.PRIVATE, EXTRACT_ALL_METHOD_NAME, null,
             new TypeDesc[] {TypeDesc.forClass(ResultSet.class), TypeDesc.INT});
        CodeBuilder b = new CodeBuilder(mi);

        defineExtract(b, b.getParameter(0), b.getParameter(1), null,
                      mInfo.getPrimaryKeyProperties().values(), lobLoaderMap);

        // Invoke extract data method to do the rest.
        b.loadThis();
        // Load the ResultSet var.
        b.loadLocal(b.getParameter(0));
        // The offset variable has already been incremented by code generated
        // by defineExtract, except for the last property.
        b.loadLocal(b.getParameter(1));
        b.loadConstant(1);
        b.math(Opcode.IADD);
        b.loadNull(); // No Lobs to update
        b.invokePrivate(EXTRACT_DATA_METHOD_NAME, null,
                        new TypeDesc[] {TypeDesc.forClass(ResultSet.class), TypeDesc.INT,
                                        TypeDesc.forClass(Lob.class).toArrayType()});

        b.returnVoid();
    }

    private void defineExtractDataMethod(Map<JDBCStorableProperty<S>, Class<?>> lobLoaderMap) {
        MethodInfo mi = mClassFile.addMethod
            (Modifiers.PRIVATE, EXTRACT_DATA_METHOD_NAME, null,
             new TypeDesc[] {TypeDesc.forClass(ResultSet.class), TypeDesc.INT,
                             TypeDesc.forClass(Lob.class).toArrayType()});
        CodeBuilder b = new CodeBuilder(mi);
        defineExtract(b, b.getParameter(0), b.getParameter(1), b.getParameter(2),
                      mInfo.getDataProperties().values(), lobLoaderMap);
        b.returnVoid();
    }

    private void defineExtract
        (CodeBuilder b,
         LocalVariable rsVar, LocalVariable initialOffsetVar, LocalVariable lobArrayVar,
         Iterable<JDBCStorableProperty<S>> properties,
         Map<JDBCStorableProperty<S>, Class<?>> lobLoaderMap)
    {
        LocalVariable offsetVar = null;
        int lobIndex = 0;

        for (JDBCStorableProperty<S> property : properties) {
            if (!property.isSelectable()) {
                continue;
            }

            // Push this in preparation for calling setXxx method.
            b.loadThis();

            b.loadLocal(rsVar);
            if (offsetVar == null) {
                offsetVar = initialOffsetVar;
            } else {
                b.integerIncrement(offsetVar, 1);
            }
            b.loadLocal(offsetVar);
            Method resultSetGetMethod = property.getResultSetGetMethod();
            b.invoke(resultSetGetMethod);

            TypeDesc resultSetType = TypeDesc.forClass(resultSetGetMethod.getReturnType());

            Label wasNull = b.createLabel();
            if (resultSetType.isPrimitive() && property.isNullable()) {
                b.loadLocal(rsVar);
                b.invokeInterface
                    (TypeDesc.forClass(ResultSet.class), "wasNull", TypeDesc.BOOLEAN, null);
                Label wasNotNull = b.createLabel();
                // boolean value is false (==0) when was not null.
                b.ifZeroComparisonBranch(wasNotNull, "==");

                // Discard result and replace with null.
                if (resultSetType.isDoubleWord()) {
                    b.pop2();
                } else {
                    b.pop();
                }
                b.loadNull();
                b.branch(wasNull);

                wasNotNull.setLocation();
            }

            if (Lob.class.isAssignableFrom(property.getType()) ||
                java.sql.Blob.class.isAssignableFrom(resultSetType.toClass()) ||
                java.sql.Clob.class.isAssignableFrom(resultSetType.toClass())) {

                // Run special conversion and then lie about the result set type.

                boolean isClob =
                    com.amazon.carbonado.lob.Clob.class.isAssignableFrom(property.getType()) ||
                    java.sql.Clob.class.isAssignableFrom(resultSetType.toClass());

                String lobTypeName = isClob ? "Clob" : "Blob";

                Method convertMethod;
                try {
                    String loaderName =
                        "com.amazon.carbonado.repo.jdbc.JDBC" + lobTypeName + "Loader";
                    convertMethod = JDBCSupport.class.getMethod
                        ("convert".concat(lobTypeName),
                         resultSetType.toClass(), Class.forName(loaderName));
                } catch (ClassNotFoundException e) {
                    throw new UndeclaredThrowableException(e);
                } catch (NoSuchMethodException e) {
                    throw new UndeclaredThrowableException(e);
                }

                pushJDBCSupport(b);
                b.swap();

                // Instantiate loader, which may be used later to reload the
                // lob. Loader is passed to convert method, where it is saved
                // inside the converted lob for future use.
                TypeDesc lobLoaderType = TypeDesc.forClass(lobLoaderMap.get(property));
                b.newObject(lobLoaderType);
                b.dup();
                b.loadThis();
                b.invokeConstructor(lobLoaderType, new TypeDesc[] {mClassFile.getType()});

                b.invoke(convertMethod);
                resultSetType = TypeDesc.forClass(convertMethod.getReturnType());

                if (lobArrayVar != null) {
                    // Add code to check if Lob needs to be updated.
                    b.loadLocal(lobArrayVar);
                    Label noUpdateLob = b.createLabel();
                    b.ifNullBranch(noUpdateLob, true);

                    b.loadLocal(lobArrayVar);
                    b.loadConstant(lobIndex);
                    b.loadFromArray(TypeDesc.OBJECT);
                    b.ifNullBranch(noUpdateLob, true);

                    // The Lob in the array represents the new value. What is
                    // currently on the stack (as converted above) is the old
                    // value currently in the database. Call the JDBCRepository
                    // updateXlob method, which stuffs the new blob contents
                    // into the old blob, thus updating it.

                    TypeDesc lobType = TypeDesc.forClass(convertMethod.getReturnType());
                    LocalVariable lob = b.createLocalVariable(null, lobType);
                    b.storeLocal(lob);

                    pushJDBCSupport(b);
                    b.loadLocal(lob);

                    b.loadLocal(lobArrayVar);
                    b.loadConstant(lobIndex);
                    b.loadFromArray(TypeDesc.OBJECT);
                    b.checkCast(lobType);

                    TypeDesc[] params = {lobType, lobType};
                    b.invokeInterface(TypeDesc.forClass(JDBCSupport.class),
                                      "update".concat(lobTypeName), null, params);

                    // Lob content now updated.
                    b.loadLocal(lob);

                    noUpdateLob.setLocation();

                    lobIndex++;
                }
            }

            TypeDesc superType = TypeDesc.forClass(mClassFile.getSuperClassName());

            StorablePropertyAdapter adapter = property.getAppliedAdapter();
            if (adapter == null) {
                TypeDesc propertyType = TypeDesc.forClass(property.getType());
                b.convert(resultSetType, propertyType);
                wasNull.setLocation();
                // Set protected field directly, since no adapter.
                b.storeField(superType, property.getName(), propertyType);
            } else {
                Method adaptMethod = adapter.findAdaptMethod
                    (resultSetType.toClass(), property.getType());
                TypeDesc adaptType = TypeDesc.forClass(adaptMethod.getParameterTypes()[0]);
                b.convert(resultSetType, adaptType);
                wasNull.setLocation();
                // Invoke special inherited protected method that invokes the
                // adapter and sets the field. Method was generated by StorableGenerator.
                b.invokeVirtual(superType,
                                property.getWriteMethodName() + '$',
                                null, new TypeDesc[] {adaptType});
            }
        }
    }

    /**
     * @param b builder to receive statement
     * @param withVersion when false, ignore any version property
     * @return version property number, or -1 if none
     */
    private int createInsertStatement(StringBuilder b, boolean withVersion) {
        b.append("INSERT INTO ");
        b.append(mInfo.getQualifiedTableName());
        b.append(" (");

        JDBCStorableProperty<?> versionProperty = null;
        int versionPropNumber = -1;

        int ordinal = 0;
        int propNumber = -1;
        for (JDBCStorableProperty<?> property : mInfo.getAllProperties().values()) {
            propNumber++;
            if (!property.isSelectable()) {
                continue;
            }
            if (property.isVersion()) {
                if (withVersion) {
                    versionProperty = property;
                    versionPropNumber = propNumber;
                }
                continue;
            }
            if (ordinal > 0) {
                b.append(',');
            }
            b.append(property.getColumnName());
            ordinal++;
        }

        // Insert version property at end, to make things easier for when the
        // proper insert statement is selected.
        if (versionProperty != null) {
            if (ordinal > 0) {
                b.append(',');
            }
            b.append(versionProperty.getColumnName());
            ordinal++;
        }

        b.append(") VALUES (");

        for (int i=0; i<ordinal; i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append('?');
        }

        b.append(')');

        return versionPropNumber;
    }

    private Map<JDBCStorableProperty<S>, Class<?>> generateLobLoaders() {
        Map<JDBCStorableProperty<S>, Class<?>> lobLoaderMap =
            new IdentityHashMap<JDBCStorableProperty<S>, Class<?>>();

        for (JDBCStorableProperty<S> property : mAllProperties.values()) {
            if (!property.isSelectable() || property.isVersion()) {
                continue;
            }

            Class psClass = property.getPreparedStatementSetMethod().getParameterTypes()[1];

            Class<?> lobLoader;

            if (com.amazon.carbonado.lob.Blob.class.isAssignableFrom(property.getType()) ||
                java.sql.Blob.class.isAssignableFrom(psClass)) {

                lobLoader = generateLobLoader(property, JDBCBlobLoader.class);
            } else if (com.amazon.carbonado.lob.Clob.class.isAssignableFrom(property.getType()) ||
                       java.sql.Clob.class.isAssignableFrom(psClass)) {

                lobLoader = generateLobLoader(property, JDBCClobLoader.class);
            } else {
                continue;
            }

            lobLoaderMap.put(property, lobLoader);
        }

        return lobLoaderMap;
    }

    /**
     * Generates an inner class conforming to JDBCBlobLoader or JDBCClobLoader.
     *
     * @param loaderType either JDBCBlobLoader or JDBCClobLoader
     */
    private Class<?> generateLobLoader(JDBCStorableProperty<S> property, Class<?> loaderType) {
        ClassInjector ci = ClassInjector.create
            (property.getEnclosingType().getName(), mParentClassLoader);

        ClassFile cf = new ClassFile(ci.getClassName());
        cf.markSynthetic();
        cf.setSourceFile(JDBCStorableGenerator.class.getName());
        cf.setTarget("1.5");
        cf.addInterface(loaderType);

        boolean isClob = loaderType == JDBCClobLoader.class;

        final TypeDesc jdbcRepoType = TypeDesc.forClass(JDBCRepository.class);
        final TypeDesc resultSetType = TypeDesc.forClass(ResultSet.class);
        final TypeDesc preparedStatementType = TypeDesc.forClass(PreparedStatement.class);
        final TypeDesc sqlLobType = TypeDesc.forClass
            (isClob ? java.sql.Clob.class : java.sql.Blob.class);

        final String enclosingFieldName = "enclosing";
        final TypeDesc enclosingType = mClassFile.getType();

        cf.addField(Modifiers.PRIVATE, enclosingFieldName, enclosingType);

        // Add constructor that accepts reference to enclosing storable.
        {
            MethodInfo mi = cf.addConstructor(Modifiers.PUBLIC, new TypeDesc[] {enclosingType});
            CodeBuilder b = new CodeBuilder(mi);
            b.loadThis();
            b.invokeSuperConstructor(null);
            b.loadThis();
            b.loadLocal(b.getParameter(0));
            b.storeField(enclosingFieldName, enclosingType);
            b.returnVoid();
        }

        MethodInfo mi = cf.addMethod
            (Modifiers.PUBLIC, "load", sqlLobType, new TypeDesc[] {jdbcRepoType});
        mi.addException(TypeDesc.forClass(FetchException.class));
        CodeBuilder b = new CodeBuilder(mi);

        LocalVariable repoVar = b.getParameter(0);

        Label tryBeforeCon = b.createLabel().setLocation();
        LocalVariable conVar = getConnection(b, repoVar);
        Label tryAfterCon = b.createLabel().setLocation();

        StringBuilder selectBuilder = new StringBuilder();
        selectBuilder.append("SELECT ");
        selectBuilder.append(property.getColumnName());
        selectBuilder.append(" FROM ");
        selectBuilder.append(mInfo.getQualifiedTableName());

        LocalVariable psVar = b.createLocalVariable("ps", preparedStatementType);

        LocalVariable instanceVar = b.createLocalVariable(null, enclosingType);
        b.loadThis();
        b.loadField(enclosingFieldName, enclosingType);
        b.storeLocal(instanceVar);

        Label tryAfterPs = buildWhereClauseAndPreparedStatement
            (b, selectBuilder, conVar, psVar, repoVar, instanceVar);

        b.loadLocal(psVar);
        b.invokeInterface(preparedStatementType, "executeQuery", resultSetType, null);
        LocalVariable rsVar = b.createLocalVariable("rs", resultSetType);
        b.storeLocal(rsVar);
        Label tryAfterRs = b.createLabel().setLocation();

        // If no results, then return null. Otherwise, there must be exactly
        // one result.

        LocalVariable resultVar = b.createLocalVariable(null, sqlLobType);
        b.loadNull();
        b.storeLocal(resultVar);

        b.loadLocal(rsVar);
        b.invokeInterface(resultSetType, "next", TypeDesc.BOOLEAN, null);
        Label noResults = b.createLabel();
        b.ifZeroComparisonBranch(noResults, "==");

        b.loadLocal(rsVar);
        b.loadConstant(1);
        b.invokeInterface(resultSetType, isClob ? "getClob" : "getBlob",
                          sqlLobType, new TypeDesc[] {TypeDesc.INT});
        b.storeLocal(resultVar);

        noResults.setLocation();

        closeResultSet(b, rsVar, tryAfterRs);
        closeStatement(b, psVar, tryAfterPs);
        yieldConAndHandleException(b, repoVar, tryBeforeCon, conVar, tryAfterCon, false);

        b.loadLocal(resultVar);
        b.returnValue(sqlLobType);

        return ci.defineClass(cf);
    }
}