package com.provectus.kafka.ui.config.auth;

import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.security.web.server.WebFilterExchange;
import org.springframework.security.web.server.authentication.RedirectServerAuthenticationSuccessHandler;
import org.springframework.security.web.server.authentication.ServerAuthenticationSuccessHandler;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

public class RbacAuthenticationSuccessHandler implements ServerAuthenticationSuccessHandler {

  private final RedirectServerAuthenticationSuccessHandler defaultHandler =
      new RedirectServerAuthenticationSuccessHandler();

  @Override
  public Mono<Void> onAuthenticationSuccess(WebFilterExchange webFilterExchange, Authentication authentication) {
    return this.process(authentication, webFilterExchange.getExchange())
        .then(defaultHandler.onAuthenticationSuccess(webFilterExchange, authentication));
  }

  private Mono<Void> process(Authentication authentication, ServerWebExchange swe) {
    if (!(authentication instanceof OAuth2AuthenticationToken)) {
      return Mono.empty();
    }

    OAuth2User principal = (OAuth2User) authentication.getPrincipal();
    if (principal instanceof RbacOidcUser user) {
      return swe.getSession()
          .doOnSuccess(s -> s.getAttributes().put("GROUPS", user.groups())).then();
    }
    if (principal instanceof RbacOAuth2User user) {
      return swe.getSession()
          .doOnSuccess(s -> s.getAttributes().put("GROUPS", user.groups())).then();
    }

    return Mono.empty();
  }

}
