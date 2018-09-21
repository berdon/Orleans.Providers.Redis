using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Shared
{
    public static class AssertEx
    {
        public static void ThrowsAny<TException>(Action action, Func<TException, bool> verifier, string message = null)
            where TException : Exception
        {
            ThrowsAny(action, verifier != null ? e => Assert.True(verifier(e)) : (Action<TException>) null, message);
        }

        public static void ThrowsAny<TException>(Action action, Action<TException> verifier = null, string message = null)
            where TException : Exception
        {
            try
            {
                action.Invoke();
            }
            catch (TException e)
            {
                verifier?.Invoke(e);
                return;
            }
            catch (Exception e)
            {
                throw new Exception($"Expected action to throw {typeof(TException).Name} typed exception; instead a {e.GetType().Name} was thrown.");
            }

            if (!string.IsNullOrEmpty(message))
            {
                throw new Exception(message, new Exception($"Expected action to throw {typeof(TException).Name} typed exception; instead none was thrown."));
            }
            else
            {
                throw new Exception($"Expected action to throw {typeof(TException).Name} typed exception; instead none was thrown.");
            }
        }

        public static Task ThrowsAnyAsync<TException>(Func<Task> action, Func<TException, bool> verifier, string message = null)
            where TException : Exception
        {
            return ThrowsAnyAsync(action, verifier != null ? e => Assert.True(verifier(e)) : (Action<TException>)null, message);
        }

        public static async Task ThrowsAnyAsync<TException>(Func<Task> action, Action<TException> verifier = null, string message = null)
            where TException : Exception
        {
            try
            {
                await action.Invoke();
            }
            catch (TException e)
            {
                verifier?.Invoke(e);
                return;
            }
            catch (Exception e)
            {
                throw new Exception($"Expected action to throw {typeof(TException).Name} typed exception; instead a {e.GetType().Name} was thrown.");
            }

            if (!string.IsNullOrEmpty(message))
            {
                throw new Exception(message, new Exception($"Expected action to throw {typeof(TException).Name} typed exception; instead none was thrown."));
            }
            else
            {
                throw new Exception($"Expected action to throw {typeof(TException).Name} typed exception; instead none was thrown.");
            }
        }

        public static void ThrowsAny<TException, TArg1, TArg2, TArg3>(Action<TArg1, TArg2, TArg3> action, TArg1 arg1, TArg2 arg2, TArg3 arg3, string message = null)
            where TException : Exception
        {
            try
            {
                action.Invoke(arg1, arg2, arg3);
            }
            catch (TException)
            {
                return;
            }
            catch (Exception e)
            {
                throw new Exception($"Expected action({arg1}, {arg2}, {arg3}) to throw {typeof(TException).Name} typed exception; instead a {e.GetType().Name} was thrown.");
            }

            if (!string.IsNullOrEmpty(message))
            {
                throw new Exception(message, new Exception($"Expected action({arg1}, {arg2}, {arg3}) to throw {typeof(TException).Name} typed exception; instead none was thrown."));
            }
            else
            {
                throw new Exception($"Expected action({arg1}, {arg2}, {arg3}) to throw {typeof(TException).Name} typed exception; instead none was thrown.");
            }
        }

        public static void Equal<T>(IEnumerable<T> expected, IEnumerable<T> actual)
        {
            var exceptions = new List<Exception>();

            try
            {
                Assert.Equal(expected.Count(), actual.Count());
            }
            catch (Exception e)
            {
                exceptions.Add(e);
            }

            var expectedEnumerator = expected.GetEnumerator();
            var actualEnumerator = actual.GetEnumerator();

            while (expectedEnumerator.MoveNext() && actualEnumerator.MoveNext())
            {
                try
                {
                    Assert.Equal(expectedEnumerator.Current, actualEnumerator.Current);
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                }
            }

            var excessExpectedItems = new List<T>();
            while (expectedEnumerator.MoveNext())
            {
                excessExpectedItems.Add(expectedEnumerator.Current);
            }

            if (excessExpectedItems.Any())
            {
                exceptions.Add(new ExpectedItemsInEnumeration<T>(excessExpectedItems));
            }

            var unexpectedItems = new List<T>();
            while (actualEnumerator.MoveNext())
            {
                unexpectedItems.Add(actualEnumerator.Current);
            }

            if (unexpectedItems.Any())
            {
                exceptions.Add(new UnexpectedItemsInEnumeration<T>(excessExpectedItems));
            }

            if (exceptions.Any())
            {
                throw new AggregateException(exceptions);
            }
        }
    }

    public class ExpectedItemsInEnumeration<T> : Exception
    {
        public IList<T> ExpectedItems { get; }

        public ExpectedItemsInEnumeration(IList<T> expectedItems) : base($"Expected {expectedItems.Count} additional items missing from actual enumeration")
        {
            ExpectedItems = expectedItems;
        }
    }

    public class UnexpectedItemsInEnumeration<T> : Exception
    {
        public IList<T> UnexpectedItems { get; }

        public UnexpectedItemsInEnumeration(IList<T> unexpectedItems) : base($"Unexpected {unexpectedItems.Count} items found in actual enumeration")
        {
            UnexpectedItems = unexpectedItems;
        }
    }
}
