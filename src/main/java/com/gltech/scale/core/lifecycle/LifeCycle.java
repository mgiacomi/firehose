package com.gltech.scale.core.lifecycle;

/**
 * By implementing this interface, a class is specifying that it has resources
 * which need to be shut down.
 * To have your resources managed automatically, register it with LifeCycleManager.
 * Also, you may want to use LifeCycleService as a delegate.
 */
public interface LifeCycle
{
	void shutdown();

	enum Priority
	{
		INITIAL,
		SECOND,
		THIRD,
		FINAL
	}
}
