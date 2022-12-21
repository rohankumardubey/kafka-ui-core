package com.provectus.kafka.ui.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.provectus.kafka.ui.service.rbac.AccessControlService;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;

public class AccessControlServiceMock {

  private AccessControlService mock = Mockito.mock(AccessControlService.class);

  public AccessControlService getMock() {
    this.mock = Mockito.mock(AccessControlService.class);

    when(mock.validateAccess(any())).thenReturn(Mono.empty());
    when(mock.isSchemaAccessible(anyString(), anyString(), any())).thenReturn(Mono.just(true));

    when(mock.isTopicAccessible(any(), anyString(), any())).thenReturn(Mono.just(true));

    return mock;
  }
}
