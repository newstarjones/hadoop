/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.factories.impl.pb;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RpcServerFactory;

import com.google.protobuf.BlockingService;

@Private
public class RpcServerFactoryPBImpl implements RpcServerFactory {

  private static final Log LOG = LogFactory.getLog(RpcServerFactoryPBImpl.class);
  private static final String PROTO_GEN_PACKAGE_NAME = "org.apache.hadoop.yarn.proto";
  private static final String PROTO_GEN_CLASS_SUFFIX = "Service";
  private static final String PB_IMPL_PACKAGE_SUFFIX = "impl.pb.service";
  private static final String PB_IMPL_CLASS_SUFFIX = "PBServiceImpl";
  
  private static final RpcServerFactoryPBImpl self = new RpcServerFactoryPBImpl();

  private Configuration localConf = new Configuration();
  private ConcurrentMap<Class<?>, Constructor<?>> serviceCache = new ConcurrentHashMap<Class<?>, Constructor<?>>();
  private ConcurrentMap<Class<?>, Method> protoCache = new ConcurrentHashMap<Class<?>, Method>();
  
  public static RpcServerFactoryPBImpl get() {
    return RpcServerFactoryPBImpl.self;
  }
  

  private RpcServerFactoryPBImpl() {
  }
  
  public Server getServer(Class<?> protocol, Object instance,
      InetSocketAddress addr, Configuration conf,
      SecretManager<? extends TokenIdentifier> secretManager, int numHandlers) {
    return getServer(protocol, instance, addr, conf, secretManager, numHandlers,
        null);
  }
  
  @Override
  public Server getServer(Class<?> protocol, Object instance,
      InetSocketAddress addr, Configuration conf,
      SecretManager<? extends TokenIdentifier> secretManager, int numHandlers,
      String portRangeConfig) {
    // 例如 这里的 instance = ApplicationMasterService，表示实现了具体业务逻辑的那个实现
    // protocol = org.apache.hadoop.yarn.api.ApplicationMasterProtocol
    // constructor = org.apache.hadoop.yarn.api.impl.pb.service.ApplicationMasterProtocolPBServiceImpl

    Constructor<?> constructor = serviceCache.get(protocol);
    if (constructor == null) {
      Class<?> pbServiceImplClazz = null;
      try {
//      得到的形式如：pbServiceImplClazz = org.apache.hadoop.yarn.api.impl.pb.service.ApplicationMasterProtocolPBServiceImpl
        pbServiceImplClazz = localConf
            .getClassByName(getPbServiceImplClassName(protocol));
      } catch (ClassNotFoundException e) {
        throw new YarnRuntimeException("Failed to load class: ["
            + getPbServiceImplClassName(protocol) + "]", e);
      }
      try {
        constructor = pbServiceImplClazz.getConstructor(protocol);
        constructor.setAccessible(true);
        serviceCache.putIfAbsent(protocol, constructor);
      } catch (NoSuchMethodException e) {
        throw new YarnRuntimeException("Could not find constructor with params: "
            + Long.TYPE + ", " + InetSocketAddress.class + ", "
            + Configuration.class, e);
      }
    }
    
    Object service = null;
    try {
      service = constructor.newInstance(instance);
    } catch (InvocationTargetException e) {
      throw new YarnRuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new YarnRuntimeException(e);
    } catch (InstantiationException e) {
      throw new YarnRuntimeException(e);
    }

    // pbProtocal = org.apache.hadoop.yarn.api.ApplicationMasterProtocolPB  pb形式的Protocol
    Class<?> pbProtocol = service.getClass().getInterfaces()[0];
    Method method = protoCache.get(protocol);
    if (method == null) {
      Class<?> protoClazz = null;
      try {
        // protoClazz = org.apache.hadoop.yarn.proto.ApplicationMasterProtocol$ApplicationMasterProtocolService
        protoClazz = localConf.getClassByName(getProtoClassName(protocol));
      } catch (ClassNotFoundException e) {
        throw new YarnRuntimeException("Failed to load class: ["
            + getProtoClassName(protocol) + "]", e);
      }
      try {
        method = protoClazz.getMethod("newReflectiveBlockingService",
            pbProtocol.getInterfaces()[0]);
        method.setAccessible(true);
        protoCache.putIfAbsent(protocol, method);
      } catch (NoSuchMethodException e) {
        throw new YarnRuntimeException(e);
      }
    }
    
    try {
      return createServer(pbProtocol, addr, conf, secretManager, numHandlers,
          (BlockingService)method.invoke(null, service), portRangeConfig);
    } catch (InvocationTargetException e) {
      throw new YarnRuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new YarnRuntimeException(e);
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
  }

  /**
   * 输入：org.apache.hadoop.yarn.api.ApplicationMasterProtocol
   * 输出：org.apache.hadoop.yarn.proto.ApplicationMasterProtocol$ApplicationMasterProtocolService
   * @param clazz
   * @return
   */
  private String getProtoClassName(Class<?> clazz) {
    String srcClassName = getClassName(clazz);
    return PROTO_GEN_PACKAGE_NAME + "." + srcClassName + "$" + srcClassName + PROTO_GEN_CLASS_SUFFIX;  
  }

  /**
   * 比如：
   * 输入 org.apache.hadoop.yarn.api.ApplicationMasterProtocol
   * 输出 org.apache.hadoop.yarn.api.impl.pb.service.ApplicationMasterProtocolPBServiceImpl
   * @param clazz
   * @return
   */
  private String getPbServiceImplClassName(Class<?> clazz) {
    String srcPackagePart = getPackageName(clazz);
    String srcClassName = getClassName(clazz);
    String destPackagePart = srcPackagePart + "." + PB_IMPL_PACKAGE_SUFFIX;
    String destClassPart = srcClassName + PB_IMPL_CLASS_SUFFIX;
    return destPackagePart + "." + destClassPart;
  }
  
  private String getClassName(Class<?> clazz) {
    String fqName = clazz.getName();
    return (fqName.substring(fqName.lastIndexOf(".") + 1, fqName.length()));
  }
  
  private String getPackageName(Class<?> clazz) {
    return clazz.getPackage().getName();
  }

  private Server createServer(Class<?> pbProtocol, InetSocketAddress addr, Configuration conf, 
      SecretManager<? extends TokenIdentifier> secretManager, int numHandlers, 
      BlockingService blockingService, String portRangeConfig) throws IOException {
    RPC.setProtocolEngine(conf, pbProtocol, ProtobufRpcEngine.class);
    RPC.Server server = new RPC.Builder(conf).setProtocol(pbProtocol)
        .setInstance(blockingService).setBindAddress(addr.getHostName())
        .setPort(addr.getPort()).setNumHandlers(numHandlers).setVerbose(false)
        .setSecretManager(secretManager).setPortRangeConfig(portRangeConfig)
        .build();
    LOG.info("Adding protocol "+pbProtocol.getCanonicalName()+" to the server");
    server.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, pbProtocol, blockingService);
    return server;
  }
}
