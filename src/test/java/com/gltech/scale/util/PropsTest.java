package com.gltech.scale.util;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class PropsTest
{
	private Props props;

	@Before
	public void setUp() throws Exception
	{
		props = Props.getProps();
		props.setVerbose(true);
	}

	@Test
	public void testLoad()
	{
		props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/props.properties");
		assertEquals("this is a string value", props.get("A.string", "nothing"));
		assertEquals(9999, props.get("SomeInt", 0));
		assertEquals(true, props.get("TheBoolean", false));
		assertEquals(Arrays.asList("first", "second", "third", "fourth"),
				props.get("Listy", Collections.<String>emptyList()));
	}

	@Test
	public void testString()
	{
		assertEquals("nothing", props.get("first", "nothing"));
		props.set("first", "something");
		assertEquals("something", props.get("first", "nothing"));
		props.set("first", "another");
		assertEquals("another", props.get("first", "nothing"));
	}

	@Test
	public void testInt()
	{
		assertEquals(5, props.get("int", 5));
		props.set("int", 20);
		assertEquals(20, props.get("int", 5));
		props.set("int", 100);
		assertEquals(100, props.get("int", 5));
	}

	@Test
	public void testBoolean()
	{
		assertEquals(true, props.get("Boo", true));
		props.set("Boo", false);
		assertEquals(false, props.get("Boo", true));
		props.set("Boo", true);
		assertEquals(true, props.get("Boo", false));
	}

	@Test
	public void testList()
	{
		List<String> one = Arrays.asList("one");
		List<String> empty = Collections.emptyList();
		assertEquals(empty, props.get("list", empty));
		assertEquals(one, props.get("list", one));
		props.set("list", one);
		assertEquals(one, props.get("list", empty));

		List<String> three = Arrays.asList("one", "two", "three");
		props.set("list", three);
		assertEquals(three, props.get("list", one));

	}

}
