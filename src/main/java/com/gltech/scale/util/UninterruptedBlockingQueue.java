package com.gltech.scale.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class follows the normal BlockingQueue interface, but does not force you
 * to handle InterruptedExceptions.  Instead, it throws RuntimeInterruptedException.
 */
public class UninterruptedBlockingQueue<E> implements BlockingQueue<E>
{

	private static final Logger logger = LoggerFactory.getLogger(UninterruptedBlockingQueue.class);

	private BlockingQueue<E> delegate;

	public UninterruptedBlockingQueue(BlockingQueue<E> delegate)
	{
		this.delegate = delegate;
	}

	public UninterruptedBlockingQueue()
	{
		this(1000);
	}

	public UninterruptedBlockingQueue(int capacity)
	{
		delegate = new LinkedBlockingQueue<>(capacity);
	}

	public void put(E e)
	{
		try
		{
			delegate.put(e);
		}
		catch (InterruptedException ie)
		{
			logger.debug("interrupted", ie);
			throw new RuntimeInterruptedException(ie);
		}
	}

	public E take()
	{
		try
		{
			return delegate.take();
		}
		catch (InterruptedException ie)
		{
			logger.debug("interrupted", ie);
			throw new RuntimeInterruptedException(ie);
		}
	}

	public boolean offer(E e, long timeout, java.util.concurrent.TimeUnit unit)
	{
		try
		{
			return delegate.offer(e, timeout, unit);
		}
		catch (InterruptedException ie)
		{
			logger.debug("interrupted", ie);
			throw new RuntimeInterruptedException(ie);
		}
	}

	public E poll(long timeout, java.util.concurrent.TimeUnit unit)
	{
		try
		{
			return delegate.poll(timeout, unit);
		}
		catch (InterruptedException ie)
		{
			logger.debug("interrupted", ie);
			throw new RuntimeInterruptedException(ie);
		}
	}

	public boolean add(E e)
	{
		return delegate.add(e);
	}

	public boolean offer(E e)
	{
		return delegate.offer(e);
	}

	public int remainingCapacity()
	{
		return delegate.remainingCapacity();
	}

	public boolean remove(Object o)
	{
		return delegate.remove(o);
	}

	public boolean contains(Object o)
	{
		return delegate.contains(o);
	}

	public int drainTo(Collection<? super E> c)
	{
		return delegate.drainTo(c);
	}

	public int drainTo(Collection<? super E> c, int maxElements)
	{
		return delegate.drainTo(c, maxElements);
	}

	public E remove()
	{
		return delegate.remove();
	}

	public E poll()
	{
		return delegate.poll();
	}

	public E element()
	{
		return delegate.element();
	}

	public E peek()
	{
		return delegate.peek();
	}

	public int size()
	{
		return delegate.size();
	}

	public boolean isEmpty()
	{
		return delegate.isEmpty();
	}

	public Iterator<E> iterator()
	{
		return delegate.iterator();
	}

	public Object[] toArray()
	{
		return delegate.toArray();
	}

	public <T> T[] toArray(T[] a)
	{
		return delegate.toArray(a);
	}

	public boolean containsAll(Collection<?> c)
	{
		return delegate.containsAll(c);
	}

	public boolean addAll(Collection<? extends E> c)
	{
		return delegate.addAll(c);
	}

	public boolean removeAll(Collection<?> c)
	{
		return delegate.removeAll(c);
	}

	public boolean retainAll(Collection<?> c)
	{
		return delegate.retainAll(c);
	}

	public void clear()
	{
		delegate.clear();
	}

	public boolean equals(Object o)
	{
		return delegate.equals(o);
	}

	public int hashCode()
	{
		return delegate.hashCode();
	}
}
