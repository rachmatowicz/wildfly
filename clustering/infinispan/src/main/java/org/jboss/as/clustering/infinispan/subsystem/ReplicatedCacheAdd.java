package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ADD;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.FluentConfiguration;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.controller.registry.Resource;
import org.jboss.as.naming.deployment.ContextNames;
import org.jboss.dmr.ModelNode;
import org.jboss.logging.Logger;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceTarget;

/**
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 */
public class ReplicatedCacheAdd extends ClusteredCacheAdd implements DescriptionProvider {

    private static final Logger log = Logger.getLogger(ReplicatedCacheAdd.class.getPackage().getName());
    static final ReplicatedCacheAdd INSTANCE = new ReplicatedCacheAdd();

    // used to create subsystem description
    static ModelNode createOperation(ModelNode address, ModelNode existing) {
        ModelNode operation = Util.getEmptyOperation(ADD, address);
        CacheAdd.populate(existing, operation);
        ClusteredCacheAdd.populate(existing, operation);
        populate(existing, operation) ;
        return operation;
    }

    @Override
    protected void populateModel(ModelNode operation, ModelNode model) throws OperationFailedException {
        // transfer the model data from operation to model
        log.debug("Populating model");
        populateClusteredCacheModelNode(operation, model);
        populate(operation, model);
        log.debug("Populated model: " + model.asString());
    }

    protected static void populate(ModelNode operation, ModelNode model) {

        // additional child node
        if (operation.hasDefined(ModelKeys.STATE_TRANSFER)) {
            model.get(ModelKeys.STATE_TRANSFER).set(operation.get(ModelKeys.STATE_TRANSFER)) ;
        }
    }

    @Override
    protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model, ServiceVerificationHandler verificationHandler, List<ServiceController<?>> newControllers) throws OperationFailedException {
        log.debug("Performing runtime") ;

        // create a Configuration holding the operation data
        Configuration overrides = new Configuration();
        // create a list for dependencies which may need to be added during processing
        List<CacheAdd.AdditionalDependency> additionalDeps = new LinkedList<AdditionalDependency>();

        // TODO
        processReplicatedCacheModelNode(model, overrides, additionalDeps);

        // this stuff can go into a common routine in CacheAdd

        // get container and cache addresses
        PathAddress cacheAddress = PathAddress.pathAddress(operation.get(OP_ADDR)) ;
        PathAddress containerAddress = cacheAddress.subAddress(0, cacheAddress.size()-1) ;

        // get container and cache names
        String cacheName = cacheAddress.getLastElement().getValue() ;
        String containerName = containerAddress.getLastElement().getValue() ;

        // get container and cache service names
        ServiceName containerServiceName = EmbeddedCacheManagerService.getServiceName(containerName) ;
        ServiceName cacheServiceName = containerServiceName.append(cacheName) ;

        // get container Model
        Resource rootResource = context.getRootResource() ;
        ModelNode container = rootResource.navigate(containerAddress).getModel() ;

        // get default cache of the container
        String defaultCache = container.require(ModelKeys.DEFAULT_CACHE).asString() ;

        // get start mode of the cache
        StartMode startMode = operation.hasDefined(ModelKeys.START) ? StartMode.valueOf(operation.get(ModelKeys.START).asString()) : StartMode.LAZY;

        // get the JNDI name of the container and its binding info
        String jndiName = CacheContainerAdd.getContainerJNDIName(container, containerName);
        final ContextNames.BindInfo bindInfo = ContextNames.bindInfoFor(jndiName) ;

        // install the cache service
        ServiceTarget target = context.getServiceTarget() ;
        // create the CacheService name
        ServiceName serviceName = EmbeddedCacheManagerService.getServiceName(containerName).append(cacheName) ;
        // create the CacheService instance
        // need to add in overrides
        ServiceBuilder<Cache<Object, Object>> builder = new CacheService<Object, Object>(cacheName, overrides).build(target, containerServiceName) ;
        builder.addDependency(bindInfo.getBinderServiceName()) ;

        // add in any additional dependencies
        for (AdditionalDependency dep : additionalDeps) {
            builder.addDependency(dep.getName(), dep.getType(), dep.getTarget()) ;
        }

        builder.setInitialMode(startMode.getMode());
        // add an alias for the default cache
        if (cacheName.equals(defaultCache)) {
            builder.addAliases(CacheService.getServiceName(containerName,  null));
        }
        // blah
        if (startMode.getMode() == ServiceController.Mode.ACTIVE) {
            builder.addListener(verificationHandler);
        }

        // if we are clustered, update TransportRequiredService  via its reference
        setTransportRequired(context, containerServiceName);

        newControllers.add(builder.install());
        log.debug("cache " + cacheName + " installed for container " + containerName);

        log.debug("Performed runtime") ;
    }

    Configuration processReplicatedCacheModelNode(ModelNode cache, Configuration configuration, List<AdditionalDependency> additionalDeps) {
        // process the basic clustered configuration
        processClusteredCacheModelNode(cache, configuration, additionalDeps);

        // process the replicated-cache attributes and elements
        FluentConfiguration fluent = configuration.fluent();
        if (cache.hasDefined(ModelKeys.STATE_TRANSFER)) {
            ModelNode stateTransfer = cache.get(ModelKeys.STATE_TRANSFER) ;
            FluentConfiguration.StateRetrievalConfig fluentStateTransfer = fluent.stateRetrieval();
            if (stateTransfer.hasDefined(ModelKeys.ENABLED)) {
                fluentStateTransfer.fetchInMemoryState(stateTransfer.get(ModelKeys.ENABLED).asBoolean());
            }
            if (stateTransfer.hasDefined(ModelKeys.TIMEOUT)) {
                fluentStateTransfer.timeout(stateTransfer.get(ModelKeys.TIMEOUT).asLong());
            }
            if (stateTransfer.hasDefined(ModelKeys.FLUSH_TIMEOUT)) {
                fluentStateTransfer.logFlushTimeout(stateTransfer.get(ModelKeys.FLUSH_TIMEOUT).asLong());
            }
        }
        return configuration;
    }

    public ModelNode getModelDescription(Locale locale) {
        return InfinispanDescriptions.getReplicatedCacheAddDescription(locale);
    }
}
