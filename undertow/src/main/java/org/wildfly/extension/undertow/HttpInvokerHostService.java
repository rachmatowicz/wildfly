/*
 * Copyright The WildFly Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.wildfly.extension.undertow;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.undertow.security.handlers.AuthenticationCallHandler;
import io.undertow.security.handlers.AuthenticationConstraintHandler;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.server.session.SecureRandomSessionIdGenerator;
import io.undertow.server.session.SessionConfig;
import io.undertow.server.session.SessionCookieConfig;
import io.undertow.servlet.api.SessionConfigWrapper;
import io.undertow.util.HttpString;
import io.undertow.util.StatusCodes;

import org.jboss.msc.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;
import org.wildfly.elytron.web.undertow.server.ElytronContextAssociationHandler;
import org.wildfly.elytron.web.undertow.server.ElytronHttpExchange;
import org.wildfly.extension.undertow.session.AffinitySessionConfigWrapper;
import org.wildfly.extension.undertow.session.AffinitySessionIdentifierCodec;
import org.wildfly.extension.undertow.session.CodecSessionConfigWrapper;
import org.wildfly.extension.undertow.session.NonDistributableSessionAffinityProvider;
import org.wildfly.extension.undertow.session.SessionAffinityProvider;
import org.wildfly.extension.undertow.logging.UndertowLogger;
import org.wildfly.httpclient.common.ElytronIdentityHandler;
import org.wildfly.security.auth.server.HttpAuthenticationFactory;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.security.http.HttpServerAuthenticationMechanism;

/**
 * @author Stuart Douglas
 * @author <a href="mailto:ropalka@redhat.com">Richard Opalka</a>
 * @author <a href="mailto:rachmato@redhat.com">Richard Achmatowicz</a>
 */
final class HttpInvokerHostService implements Service {

    private final Supplier<Host> host;
    private final Supplier<HttpAuthenticationFactory> httpAuthenticationFactory;
    private final Supplier<PathHandler> remoteHttpInvokerService;
    private final String path;

    private static final String BACKEND_PATH = "backend";
    private static final HttpString BACKEND_HEADER = new HttpString("backend");

    HttpInvokerHostService(
            final Supplier<Host> host,
            final Supplier<HttpAuthenticationFactory> httpAuthenticationFactory,
            final Supplier<PathHandler> remoteHttpInvokerService,
            final String path) {
        this.host = host;
        this.httpAuthenticationFactory = httpAuthenticationFactory;
        this.remoteHttpInvokerService = remoteHttpInvokerService;
        this.path = path;
    }

    @Override
    public void start(final StartContext startContext) {
        HttpHandler handler = remoteHttpInvokerService.get();
        handler = addBackendServerHandler(handler);
        if (httpAuthenticationFactory != null) {
            handler = secureAccess( handler, httpAuthenticationFactory.get());
        }

        SessionCookieConfig sessionConfig = new SessionCookieConfig();
        assert this.path.startsWith("/") : "path must be normalized before constructing HttpInvokerHostService";
        sessionConfig.setPath(this.path);
        Server server = this.host.get().getServer();
        ServletContainerService container = server.getServletContainer();
        CookieConfig affinityCookeConfig = container.getAffinityCookieConfig();
        CookieConfig sessionCookieConfig = container.getSessionCookieConfig();
        if (sessionCookieConfig != null) {
            if (sessionCookieConfig.getDomain() != null) {
                sessionConfig.setDomain(sessionCookieConfig.getDomain());
            }
            if (sessionCookieConfig.getMaxAge() != null) {
                sessionConfig.setMaxAge(sessionCookieConfig.getMaxAge());
            }
            if (sessionCookieConfig.getSecure() != null) {
                sessionConfig.setSecure(sessionCookieConfig.getSecure());
            }
            if (sessionCookieConfig.getHttpOnly() != null) {
                sessionConfig.setHttpOnly(sessionCookieConfig.getHttpOnly());
            }
        }
        SessionAffinityProvider affinityProvider = new NonDistributableSessionAffinityProvider(server);
        SessionConfigWrapper wrapper = (affinityCookeConfig != null) ? new AffinitySessionConfigWrapper(affinityCookeConfig, affinityProvider) : new CodecSessionConfigWrapper(new AffinitySessionIdentifierCodec(affinityProvider));

        handler = setupRoutes(handler, wrapper.wrap(sessionConfig, null));
        host.get().registerHandler(path, handler);
        host.get().registerLocation(path);
    }

    @Override
    public void stop(final StopContext stopContext) {
        host.get().unregisterHandler(path);
        host.get().unregisterLocation(path);
    }

    /**
     * Add an (anonymous) HttpHandler to attach an encoded JSESSIONID cookie to this host to the remote invoker response.
     * The session identifier codec will append the route to this host to the sessionid to permit correct routing.
     * @param handler the HttpHandler to apply the handler to
     * @return the HttpHandler with routing enabled
     */
    private HttpHandler setupRoutes(HttpHandler handler, SessionConfig sessionConfig) {
        final SecureRandomSessionIdGenerator generator = new SecureRandomSessionIdGenerator();
        return exchange -> {
            exchange.addResponseCommitListener(ex -> {
                String id = sessionConfig.findSessionId(exchange);
                if (id != null) {
                    sessionConfig.setSessionId(exchange, id);
                } else if (ex.getStatusCode() == StatusCodes.UNAUTHORIZED) {
                    // add a session cookie in order to avoid sticky session issue after 401 Unauthorized response
                    sessionConfig.setSessionId(exchange, generator.createSessionId());
                }
            });
            handler.handleRequest(exchange);
        };
    }

    /**
     * Add a security chain of handlers which will be called to authenticate a remote invoker request.
     * @param domainHandler the HttpHandler to apply the security chain to
     * @param httpAuthenticationFactory the authentication factory to use for authentication
     * @return the secured HttpHandler
     */
    private static HttpHandler secureAccess(HttpHandler domainHandler, final HttpAuthenticationFactory httpAuthenticationFactory) {
        domainHandler = new AuthenticationCallHandler(domainHandler);
        domainHandler = new AuthenticationConstraintHandler(domainHandler);
        Supplier<List<HttpServerAuthenticationMechanism>> mechanismSupplier = () ->
                httpAuthenticationFactory.getMechanismNames().stream()
                        .map(s -> {
                            try {
                                return httpAuthenticationFactory.createMechanism(s);
                            } catch (Exception e) {
                                return null;
                            }
                        })
                        .collect(Collectors.toList());
        domainHandler = ElytronContextAssociationHandler.builder()
                .setNext(domainHandler)
                .setMechanismSupplier(mechanismSupplier)
                .setHttpExchangeSupplier(h -> new ElytronHttpExchange(h) {

                    @Override
                    public void authenticationComplete(SecurityIdentity securityIdentity, String mechanismName) {
                        super.authenticationComplete(securityIdentity, mechanismName);
                        h.putAttachment(ElytronIdentityHandler.IDENTITY_KEY, securityIdentity);
                    }

                })
                .build();

        return domainHandler;
    }

    /**
     * Adds a handler to return the URL of this Host back to the caller.
     * This adjustment was required (WEJBHTTP-81) to allow a client to discover a randomly chosen backend server
     * behind a load balancer to support strict stickiness semantics.
     *
     * @param handler the HttpHandler to add the path to
     * @return the HttpHandler with the new path added
     */
    private HttpHandler addBackendServerHandler(HttpHandler handler) {
        // handler for returning the URI of this backend Host
        PathHandler pathHandler = (PathHandler) handler;
        pathHandler.addPrefixPath(BACKEND_PATH, exchange -> {
            // get the URL of the request scheme
            Server server = host.get().getServer();
            List<UndertowListener> listeners = server.getListeners();
            UndertowListener listener = listeners.stream()
                    .filter(l -> l.getProtocol().equals(exchange.getRequestScheme()))
                    .findFirst()
                    .orElse(null);
            if (listener == null) {
                throw UndertowLogger.ROOT_LOGGER.requestSchemeHasNoListener(exchange.getRequestScheme());
            }
            InetSocketAddress socketAddress = listener.getSocketBinding().getSocketAddress();

            // pass the Host id in a query string
            String queryString = String.format("name=%s", System.getProperty("jboss.node.name","localhost"));
            URI backendURI = new URI(exchange.getRequestScheme(), null, socketAddress.getHostName(), socketAddress.getPort(), null, queryString, null);

            UndertowLogger.ROOT_LOGGER.debugf("RemoteHttpInvokerService: setting backend header with url %s", backendURI.toString());
            exchange.getResponseHeaders().put(BACKEND_HEADER, backendURI.toString());
        });
        return handler;
    }

    public String getPath() {
        return path;
    }
}

