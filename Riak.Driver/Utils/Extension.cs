using System.Text;

namespace Riak.Driver.Utils
{
    /// <summary>
    /// extension
    /// </summary>
    static public class Extension
    {
        /// <summary>
        /// get bytes
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        static public byte[] GetBytes(this string value)
        {
            if (value == null) return null;
            return Encoding.UTF8.GetBytes(value);
        }
        /// <summary>
        /// get string
        /// </summary>
        /// <param name="bytes"></param>
        /// <returns></returns>
        static public string GetString(this byte[] bytes)
        {
            if (bytes == null) return null;
            return Encoding.UTF8.GetString(bytes);
        }
    }
}