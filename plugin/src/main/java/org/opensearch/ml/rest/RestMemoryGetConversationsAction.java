/*
 * Copyright 2023 Aryn
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
package org.opensearch.ml.rest;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import lombok.extern.log4j.Log4j2;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.ml.common.conversation.ActionConstants;
import org.opensearch.ml.memory.action.conversation.GetConversationsAction;
import org.opensearch.ml.memory.action.conversation.GetConversationsRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.ppl.transport.PPLQueryAction;
import org.opensearch.sql.ppl.transport.TransportPPLQueryRequest;
import org.opensearch.sql.ppl.transport.TransportPPLQueryResponse;

/**
 * Rest Handler for list conversations
 */
@Log4j2
public class RestMemoryGetConversationsAction extends BaseRestHandler {
    private final static String GET_CONVERSATIONS_NAME = "conversational_get_conversations";

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, ActionConstants.GET_CONVERSATIONS_REST_PATH));
    }

    @Override
    public String getName() {
        return GET_CONVERSATIONS_NAME;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        GetConversationsRequest lcRequest = GetConversationsRequest.fromRestRequest(request);
        String ppl = "search source=test1;";
        TransportPPLQueryRequest transportPPLQueryRequest = new TransportPPLQueryRequest(new PPLQueryRequest(ppl, null, "/_plugins/_ppl"));
        ActionListener<TransportPPLQueryResponse>  listener = new ActionListener<>() {
            @Override
            public void onResponse(TransportPPLQueryResponse response) {
                log.info("get ppl response:"+response.getResult());
            }

            @Override
            public void onFailure(Exception e) {
                log.error("get ppl failed:"+ e.toString());
            }
        };
        client.execute(PPLQueryAction.INSTANCE, transportPPLQueryRequest, getListener(listener));
        return channel -> client.execute(GetConversationsAction.INSTANCE, lcRequest, new RestToXContentListener<>(channel));
    }

    private ActionListener<TransportPPLQueryResponse> getListener(
        ActionListener<TransportPPLQueryResponse> listener
    ) {
        return wrapActionListener(listener, TransportPPLQueryResponse::fromActionResponse);
    }

    private <T extends ActionResponse> ActionListener<T> wrapActionListener(
        final ActionListener<T> listener, final Function<ActionResponse, T> recreate
    ) {
        return ActionListener.wrap(r -> listener.onResponse(recreate.apply(r)), listener::onFailure);
    }
}
