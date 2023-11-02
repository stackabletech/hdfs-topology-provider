package tech.stackable.hadoop;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for simple App.
 */
public class StackableTopologyProviderTest
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public StackableTopologyProviderTest(String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( StackableTopologyProviderTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        StackableTopologyProvider provider = new StackableTopologyProvider();
        List<String> ips = new ArrayList();
        ips.add("10.244.0.6");
        List<String> resolved = provider.resolve(ips);
        assertEquals(resolved.size(), 1);
        assertEquals("kind-control-plane", resolved.get(0));

    }
}
