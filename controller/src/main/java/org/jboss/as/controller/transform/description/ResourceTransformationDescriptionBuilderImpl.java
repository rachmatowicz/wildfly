/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2013, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.as.controller.transform.description;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ResourceDefinition;
import org.jboss.as.controller.transform.OperationTransformer;
import org.jboss.as.controller.transform.PathTransformation;
import org.jboss.as.controller.transform.ResourceTransformer;

/**
 * @author Emanuel Muckenhuber
 * @author <a href="kabir.khan@jboss.com">Kabir Khan</a>
 */
class ResourceTransformationDescriptionBuilderImpl extends AbstractTransformationDescriptionBuilder implements ResourceTransformationDescriptionBuilder {

    private DiscardPolicy discardPolicy = DiscardPolicy.NEVER;
    private final AttributeTransformationDescriptionBuilderRegistry registry = new AttributeTransformationDescriptionBuilderRegistry();

    protected ResourceTransformationDescriptionBuilderImpl(final PathElement pathElement) {
        this(pathElement, PathTransformation.DEFAULT);
    }

    protected ResourceTransformationDescriptionBuilderImpl(final PathElement pathElement, final PathTransformation pathTransformation) {
        super(pathElement, pathTransformation, ResourceTransformer.DEFAULT, OperationTransformer.DEFAULT);
    }

    @Override
    public ResourceTransformationDescriptionBuilder addChildResource(final PathElement pathElement) {
        final ResourceTransformationDescriptionBuilderImpl builder = new ResourceTransformationDescriptionBuilderImpl(pathElement);
        children.add(builder);
        return builder;
    }

    @Override
    public ResourceTransformationDescriptionBuilder addChildResource(final ResourceDefinition definition) {
        final ResourceTransformationDescriptionBuilderImpl builder = new ResourceTransformationDescriptionBuilderImpl(definition.getPathElement());
        children.add(builder);
        return builder;
    }

    @Override
    public DiscardTransformationDescriptionBuilder discardChildResource(final PathElement pathElement) {
        final DiscardTransformationDescriptionBuilder builder = TransformationDescriptionBuilder.Factory.createDiscardInstance(pathElement);
        children.add(builder);
        return builder;
    }

    @Override
    public ResourceTransformationDescriptionBuilder addChildRedirection(PathElement oldAddress, PathElement newAddress) {
        final PathTransformation transformation = new PathTransformation.BasicPathTransformation(newAddress);
        final ResourceTransformationDescriptionBuilderImpl builder = new ResourceTransformationDescriptionBuilderImpl(oldAddress, transformation);
        children.add(builder);
        return builder;
    }

    @Override
    public ResourceTransformationDescriptionBuilder setResourceTransformer(final ResourceTransformer resourceTransformer) {
        super.setResourceTransformer(resourceTransformer);
        return this;
    }

    @Override
    public TransformationDescription build() {
        // Just skip the rest, because we can
        if(discardPolicy == DiscardPolicy.ALWAYS) {
            return new DiscardDefinition(pathElement);
        }

        final List<TransformationRule> rules = new ArrayList<TransformationRule>();
        // Build attribute rules
        final Map<String, AttributeTransformationDescription> attributes = registry.buildAttributes();

        // Process children
        final List<TransformationDescription> children = new ArrayList<TransformationDescription>();
        for(final TransformationDescriptionBuilder builder : this.children) {
            children.add(builder.build());
        }
        // Create the description
        return new TransformingDescription(pathElement, pathTransformation, discardPolicy, resourceTransformer, attributes, children);
    }

    @Override
    public AttributeTransformationDescriptionBuilder<String> getStringAttributeBuilder() {
        AttributeTransformationDescriptionBuilderImpl<String> builder = new AttributeStringTransformationDescriptionBuilderImpl();
        return builder;
    }

    @Override
    public AttributeTransformationDescriptionBuilder<AttributeDefinition> getDefAttributeBuilder() {
        AttributeTransformationDescriptionBuilderImpl<AttributeDefinition> builder = new AttributeDefTransformationDescriptionBuilderImpl();
        return builder;
    }

    private static class AttributeTransformationDescriptionBuilderRegistry {
        private final Set<String> allAttributes = new HashSet<String>();
        private final Map<String, List<RejectAttributeChecker>> attributeRestrictions = new HashMap<String, List<RejectAttributeChecker>>();
        private final Map<String, DiscardAttributeChecker> discardedAttributes = new HashMap<String, DiscardAttributeChecker>();
        private final Map<String, String> renamedAttributes = new HashMap<String, String>();
        private final Map<String, AttributeConverter> convertedAttributes = new HashMap<String, AttributeConverter>();
        private final Map<String, AttributeConverter> addedAttributes = new HashMap<String, AttributeConverter>();


        void addToAllAttributes(String attributeName) {
            if (!allAttributes.contains(attributeName)) {
                allAttributes.add(attributeName);
            }
        }

        void addAttributeCheck(final String attributeName, final RejectAttributeChecker checker) {
            addToAllAttributes(attributeName);
            List<RejectAttributeChecker> checkers = attributeRestrictions.get(attributeName);
            if(checkers == null) {
                checkers = new ArrayList<RejectAttributeChecker>();
                attributeRestrictions.put(attributeName, checkers);
            }
            checkers.add(checker);
        }

        void setDiscardedAttribute(DiscardAttributeChecker discardChecker, String attributeName) {
            assert discardChecker != null : "Null discard checker";
            assert !discardedAttributes.containsKey(attributeName) : "Discard already set";
            addToAllAttributes(attributeName);
            discardedAttributes.put(attributeName, discardChecker);
        }

        void addRenamedAttribute(String attributeName, String newName) {
            assert !renamedAttributes.containsKey(attributeName) : "Rename already set";
            addToAllAttributes(attributeName);
            renamedAttributes.put(attributeName, newName);
        }

        void addAttributeConverter(String attributeName, AttributeConverter attributeConverter) {
            addToAllAttributes(attributeName);
            convertedAttributes.put(attributeName, attributeConverter);
        }

        void addAddedAttribute(String attributeName, AttributeConverter attributeConverter) {
            addToAllAttributes(attributeName);
            addedAttributes.put(attributeName, attributeConverter);
        }

        Map<String, AttributeTransformationDescription> buildAttributes(){
            Map<String, AttributeTransformationDescription> attributes = new HashMap<String, AttributeTransformationDescription>();
            for (String name : allAttributes) {
                List<RejectAttributeChecker> checkers = attributeRestrictions.get(name);
                String newName = renamedAttributes.get(name);
                DiscardAttributeChecker discardChecker = discardedAttributes.get(name);
                attributes.put(name, new AttributeTransformationDescription(name, checkers, newName, discardChecker, convertedAttributes.get(name), addedAttributes.get(name)));
            }
            return attributes;
        }
    }

    private abstract class AttributeTransformationDescriptionBuilderImpl<T> implements AttributeTransformationDescriptionBuilder<T> {
        AttributeTransformationDescriptionBuilderImpl() {
        }

        @Override
        public ResourceTransformationDescriptionBuilder end() {
            return ResourceTransformationDescriptionBuilderImpl.this;
        }

        @Override
        public AttributeTransformationDescriptionBuilder<T> setDiscard(DiscardAttributeChecker discardChecker, T...discardedAttributes) {
            T[] useDefs = discardedAttributes;
            for (T attribute : useDefs) {
                String attrName = getAttributeName(attribute);
                registry.setDiscardedAttribute(discardChecker, attrName);
            }

            return this;
        }

        @Override
        public AttributeTransformationDescriptionBuilder<T> setRejectExpressions(final T...rejectedAttributes) {
            return addRejectCheck(RejectAttributeChecker.SIMPLE_EXPRESSIONS, rejectedAttributes);
        }

        @Override
        public AttributeTransformationDescriptionBuilderImpl<T> addRejectCheck(final RejectAttributeChecker checker, final T...rejectedAttributes){
            for (T attribute : rejectedAttributes) {
                String attrName = getAttributeName(attribute);
                registry.addAttributeCheck(attrName, checker);
            }
            return this;
        }

        @Override
        public AttributeTransformationDescriptionBuilder<T> addRejectChecks(List<RejectAttributeChecker> rejectCheckers, T...rejectedAttributes) {
            for (RejectAttributeChecker rejectChecker : rejectCheckers) {
                addRejectCheck(rejectChecker, rejectedAttributes);
            }
            return this;
        }

        @Override
        public AttributeTransformationDescriptionBuilder<T> addRename(T attributeName, String newName) {
            registry.addRenamedAttribute(getAttributeName(attributeName), newName);
            return this;
        }

        public AttributeTransformationDescriptionBuilder<T> addRenames(Map<T, String> renames) {
            for (Map.Entry<T, String> rename : renames.entrySet()) {
                registry.addRenamedAttribute(getAttributeName(rename.getKey()), rename.getValue());
            }
            return this;
        }


        @Override
        public AttributeTransformationDescriptionBuilder<T> setValueConverter(AttributeConverter attributeConverter, T...convertedAttributes) {
            for (T attribute : convertedAttributes) {
                String attrName = getAttributeName(attribute);
                registry.addAttributeConverter(attrName, attributeConverter);
            }
            return this;
        }

        @Override
        public AttributeTransformationDescriptionBuilder<T> addAttribute(T attribute, AttributeConverter attributeConverter) {
            registry.addAddedAttribute(getAttributeName(attribute), attributeConverter);
            return this;
        }


        abstract String getAttributeName(T attr);
    }

    private class AttributeStringTransformationDescriptionBuilderImpl extends AttributeTransformationDescriptionBuilderImpl<String>{

        @Override
        String getAttributeName(String attr) {
            return attr;
        }
    }

    private class AttributeDefTransformationDescriptionBuilderImpl extends AttributeTransformationDescriptionBuilderImpl<AttributeDefinition>{
        @Override
        String getAttributeName(AttributeDefinition attr) {
            return attr.getName();
        }

    }
}
