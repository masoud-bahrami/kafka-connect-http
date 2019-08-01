package ir.refactor.kafka.connect.http;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class RestAPISourceTeskTests extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public RestAPISourceTeskTests( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( RestAPISourceTeskTests.class );
    }

    /**
     * Rigourous Test :-)
     */
    
    public void testApp()
    {
        RestAPISourceTask task = new RestAPISourceTask();  
        Map<String, String> props = new HashMap<String, String>();
        props.put(RestAPISourceConnector.RSS_URI, "http://localhost:37836/api/EventFeeder/feed/");
        props.put(RestAPISourceConnector.TOPIC_CONFIG,"test");
        task.start(props);
        assertTrue( true );
    }
}
