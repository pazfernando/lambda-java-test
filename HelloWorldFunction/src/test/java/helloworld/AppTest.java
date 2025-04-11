package helloworld;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import example.Hello;

public class AppTest {
  @Test
  public void successfulResponse() {
    Hello app = new Hello();
    APIGatewayProxyRequestEvent input = new APIGatewayProxyRequestEvent();
    Map<String, String> queryStringParameters = new HashMap<>();
    queryStringParameters.put("dbType", "null");
    input.setQueryStringParameters(queryStringParameters);
    APIGatewayProxyResponseEvent result = app.handleRequest(input, null);
    assertEquals(200, result.getStatusCode().intValue());
    assertEquals("application/json", result.getHeaders().get("Content-Type"));
    String content = result.getBody();
    assertNotNull(content);
    // assertTrue(content.contains("\"message\""));
    // assertTrue(content.contains("\"hello world\""));
    // assertTrue(content.contains("\"location\""));
  }
}
