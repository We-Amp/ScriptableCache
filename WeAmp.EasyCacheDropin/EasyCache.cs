using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
//using System.Runtime.Caching;
using System.Text;
using System.Web;
using CSharpTest.Net.Collections;
using System.Diagnostics;

namespace WeAmp.EasyCacheDropin
{
    public static class Log
    {
        static DateTime start = DateTime.Now;

        public static void WriteLine(string fmt, params string[] args)
        {
            string msg = String.Format(fmt, args);
            msg = String.Format("easycache {0}: {1}", (DateTime.Now - start).TotalMilliseconds, msg);
            Debug.WriteLine(fmt, msg);
        }
    }

    public class CacheItemPolicy
    {
        public DateTime AbsoluteExpiration { get; set; }
    }
    public class CacheItem
    {
        public CacheItem(string key)
        {
            this.Key = key;
        }
        public object Value;
        public string Key;
        public DateTime DateAdded;
        public DateTime ValidUntil;
    }

    public class MemoryCache
    {
        CSharpTest.Net.Collections.LurchTable<string, CacheItem> lru_;
        public MemoryCache(string name)
        {
            lru_ = new LurchTable<string, CacheItem>(100);
        }
        public void Add(CacheItem i, CacheItemPolicy p)
        {
            i.ValidUntil = p.AbsoluteExpiration;
            i.DateAdded = DateTime.Now;
            if (lru_.TryAdd(i.Key, i))
            {
                // log success & fail
            }
        }
        public CacheItem GetCacheItem(string key)
        {
            CacheItem v;
            if (lru_.TryGetValue(key, out v))
            {
                if (DateTime.Now >= v.ValidUntil)
                {
                    if (lru_.TryRemove(key, out v))
                    {
                        // log evicition / fail
                    }
                    return null;
                }
                return v;
            }
            return null;
        }

        public void PurgeAll()
        {
            lru_.Clear();
        }
    }

    class EasyCacheResponseEntry
    {
        public int status;
        public string body { get; set; }
        public NameValueCollection response_headers { get; set; }
    }


    public enum ConfigEntryMatchType
    {
        Exact,
        Wildcard
    }
    public class ConfigEntry
    {
        public static bool Parse(string s, out ConfigEntry e)
        {
            e = new ConfigEntry();
            e.TTL = 500;
            e.MatchLine = s;
            e.MatchType = s.IndexOf("*") >= 0 ? ConfigEntryMatchType.Wildcard : ConfigEntryMatchType.Exact;
            e.IsNegative = false;
            return true;
        }

        public int TTL;
        public string MatchLine;
        public ConfigEntryMatchType MatchType;
        public bool IsNegative;

    }

    public class EasyCacheHttpModule : IHttpModule
    {
        MemoryCache cache_ = null;
        FileSystemWatcher config_watcher_ = null;
        Dictionary<string, ConfigEntry> ExactConfig = new Dictionary<string, ConfigEntry>();
        List<ConfigEntry> WildcardConfig = new List<ConfigEntry>();
        object configLock = new object();

        public void Dispose()
        {

        }
        public void Init(HttpApplication app)
        {
            app.BeginRequest += App_BeginRequest;
            app.EndRequest += App_EndRequest;
            app.Error += App_Error;
            app.PreSendRequestHeaders += App_PreSendRequestHeaders;

            cache_ = new MemoryCache("EasyCache");
            config_watcher_ = new FileSystemWatcher();
            config_watcher_.BeginInit();
            config_watcher_.Path = HttpRuntime.AppDomainAppPath;
            config_watcher_.IncludeSubdirectories = false;
            config_watcher_.Changed += Config_watcher__Changed;
            config_watcher_.EnableRaisingEvents = true;
            config_watcher_.EndInit();
            ReloadConfig();
        }

        private void App_Error(object sender, EventArgs e)
        {
            var f = HttpContext.Current.Response.Filter;
            Log.WriteLine("App_Error fired");

            // TODO: bad assumption. Should store in context somewhere.
            if (f is RecordingFilter)
            {
                ((RecordingFilter)f).Fail();
            }
        }

        private void App_EndRequest(object sender, EventArgs e)
        {
            Log.WriteLine("App_EndRequest fired");
            var f = HttpContext.Current.Response.Filter;
            // TODO: bad assumption. Should store in context somewhere.
            if (f is RecordingFilter)
            {
                ((RecordingFilter)f).Done();
            }
        }

        private void Config_watcher__Changed(object sender, FileSystemEventArgs e)
        {
            Log.WriteLine("Config_watcher__Changed fired");
            ReloadConfig();
        }

        private void ReloadConfig()
        {
            Log.WriteLine("ReloadConfig()");

            
            string path = Path.Combine(HttpRuntime.AppDomainAppPath, "easycache.config");
            try
            {
                var lines = File.ReadAllLines(path);
                lock (configLock)
                    ExactConfig.Clear();
                WildcardConfig.Clear();

                foreach (var line in lines)
                {
                    string s = line ?? "";
                    s = s.Trim();
                    if (s.Length > 1)
                    {
                        if (s[0] == '#')
                            continue;

                    }
                    ConfigEntry e;
                    if (ConfigEntry.Parse(s, out e))
                    {
                        Log.WriteLine("Parsed config line: {0}", s);

                        if (e.MatchType == ConfigEntryMatchType.Exact)
                        {
                            ExactConfig[s] = e;
                        }
                        else
                        {
                            WildcardConfig.Add(e);
                        }
                    }
                    else {
                        // log
                        Log.WriteLine("Failed to parse config line: {0}", s);
                    }
                }
            }
            catch(Exception ex)
            {
                Log.WriteLine("Exception reloading config: {0}", ex.Message);
            }
        }

        private void App_PreSendRequestHeaders(object sender, EventArgs e)
        {

        }

        private void App_BeginRequest(object sender, EventArgs e)
        {
            var app = (HttpApplication)sender;
            var ctx = app.Context;
            var query = ctx.Request.Url.ToString();

            if (query.EndsWith("easycachepurge"))
            {
                Log.WriteLine("Executing purge");
                cache_.PurgeAll();
                ctx.Response.Write("purgeall OK");
                HttpContext.Current.ApplicationInstance.CompleteRequest();
            }

            ConfigEntry cfg = GetConfigEntryForQuery(query);
            if (cfg == null)
            {
                Log.WriteLine("Decline request, no configuration for request found");
                return;
            }

            CacheItem i = cache_.GetCacheItem(query);
            if (i != null)
            {
                EasyCacheResponseEntry easy_cache_entry = (EasyCacheResponseEntry)i.Value;
                if (easy_cache_entry.status == 200) {
                    Log.WriteLine("Serve cached response");
                    foreach (var key in easy_cache_entry.response_headers.AllKeys)
                    {
                        ctx.Response.Headers[key] = easy_cache_entry.response_headers[key];
                    }
                    ctx.Response.Headers["X-Cache"] = "HIT";
                    ctx.Response.Headers["X-Age"] = Math.Round((DateTime.Now - i.DateAdded).TotalSeconds).ToString();
                    ctx.Response.Write(easy_cache_entry.body);
                    HttpContext.Current.ApplicationInstance.CompleteRequest();
                } else
                {
                    Log.WriteLine("Rember recently failed or skipped recording");
                }
            }
            else
            {
                if (ctx.Request.HttpMethod != "GET"
                    || ctx.Request.Headers["if-modified-since"] != null
                    || ctx.Request.Headers["if-none-match"] != null)
                {
                    Log.WriteLine("Decline request on default request preconditions");
                    return;
                }
                Log.WriteLine("Recording response");
                var f = new RecordingFilter(ctx.Response.Filter, cache_, query, cfg);
                ctx.Response.Filter = f;
                ctx.Response.Headers["X-Cache"] = "MISS";
            }
        }


        /// <summary>
        /// Compares wildcard to string
        /// </summary>
        /// <param name="WildString">String to compare</param>
        /// <param name="Mask">Wildcard mask (ex: *.jpg)</param>
        /// <returns>True if match found</returns>
        static bool CompareWildcard(string WildString, string Mask, bool IgnoreCase = true)
        {
            int i = 0, k = 0;

            // Cannot continue with Mask empty
            if (string.IsNullOrEmpty(Mask))
                return false;

            // If WildString is null -> make it an empty string
            if (WildString == null)
                WildString = string.Empty;

            // If Mask is * and WildString isn't empty -> return true
            if (string.Compare(Mask, "*") == 0 && !string.IsNullOrEmpty(WildString))
                return true;

            // If Mask is ? and WildString length is 1 -> return true
            if (string.Compare(Mask, "?") == 0 && WildString.Length == 1)
                return true;

            // If WildString and Mask match -> no need to go any further
            if (string.Compare(WildString, Mask, IgnoreCase) == 0)
                return true;

            while (k != WildString.Length)
            {
                switch (Mask[i])
                {
                    case '*':

                        if ((i + 1) == Mask.Length)
                            return true;

                        while (k != WildString.Length)
                        {
                            if (CompareWildcard(WildString.Substring(k + 1), Mask.Substring(i + 1), IgnoreCase))
                                return true;

                            k += 1;
                        }

                        return false;

                    case '?':

                        break;

                    default:

                        if (IgnoreCase == false && WildString[k] != Mask[i])
                            return false;

                        if (IgnoreCase && Char.ToLower(WildString[k]) != Char.ToLower(Mask[i]))
                            return false;

                        break;
                }

                i += 1;
                k += 1;
            }

            if (k == WildString.Length)
            {
                if (i == Mask.Length || Mask[i] == '*')
                    return true;
            }

            return false;
        }
        private ConfigEntry GetConfigEntryForQuery(string query)
        {
            lock (configLock)
            {
                ConfigEntry e;
                if (ExactConfig.TryGetValue(query, out e)) {
                    if (e.IsNegative)
                    {
                        return null;
                    }
                    return e;
                }
                foreach (var cfg in WildcardConfig)
                {
                    if (CompareWildcard(query, cfg.MatchLine, false))
                    {
                        if (cfg.IsNegative)
                        {
                            return null;
                        }
                        return cfg;
                    }
                }
                return null;
            }
        }
    }

    public class RecordingFilter : Stream
    {
        private Stream _sink;
        StringBuilder buffer_ = new StringBuilder();
        MemoryCache cache_;
        string key_;
        NameValueCollection response_headers_;
        bool tested_;
        bool should_record_;
        bool failed_;
        bool skip_record_;
        ConfigEntry config_;

        public RecordingFilter(Stream sink, MemoryCache cache, string key, ConfigEntry cfg)
        {
            _sink = sink;
            cache_ = cache;
            key_ = key;
            response_headers_ = new NameValueCollection();
            tested_ = false;
            should_record_ = false;
            failed_ = false;
            skip_record_ = false;
            config_ = cfg;
        }

        public void Done()
        {
            if (should_record_)
            {
                if (!failed_)
                {
                    CacheItem i = new CacheItem(key_);
                    EasyCacheResponseEntry e = new EasyCacheResponseEntry();
                    e.status = 200;
                    e.body = buffer_.ToString();
                    e.response_headers = response_headers_;
                    response_headers_.Remove("last-modified");
                    response_headers_.Remove("etag");
                    response_headers_.Remove("authorization");
                    response_headers_.Remove("cookie");
                    response_headers_.Remove("set-cookie");
                    response_headers_.Remove("set-cookie2");
                    i.Value = e;
                    CacheItemPolicy p = new CacheItemPolicy();
                    p.AbsoluteExpiration = DateTime.Now.Add(new TimeSpan(0, 0, config_.TTL));
                    cache_.Add(i, p);
                    buffer_ = null;
                } else
                {
                    Log.WriteLine("Mark recording as failed in cache because failed_ is set");

                    CacheItem i = new CacheItem(key_);
                    EasyCacheResponseEntry e = new EasyCacheResponseEntry();
                    e.status = 501;
                    CacheItemPolicy p = new CacheItemPolicy();
                    p.AbsoluteExpiration = DateTime.Now.Add(new TimeSpan(0, 0, 300));
                    cache_.Add(i, p);
                }
            } else {
                Log.WriteLine("Mark recording as failed in cache because should_record_ is not set based on response headers");
                CacheItem i = new CacheItem(key_);
                EasyCacheResponseEntry e = new EasyCacheResponseEntry();
                e.status = 502;
                CacheItemPolicy p = new CacheItemPolicy();
                p.AbsoluteExpiration = DateTime.Now.Add(new TimeSpan(0, 0, 300));
                cache_.Add(i, p);
            }
        }

        public void Fail()
        {
            Log.WriteLine("Fail() called");
            failed_ = true;
        }

        public override bool CanRead
        {
            get { return _sink.CanRead; }
        }

        public override bool CanSeek
        {
            get { return _sink.CanSeek; }
        }

        public override bool CanWrite
        {
            get { return _sink.CanWrite; }
        }

        public override long Length
        {
            get { return _sink.Length; }
        }

        public override long Position
        {
            get { return _sink.Position; }
            set
            {
                _sink.Position = value;
            }
        }

        public override long Seek(long offset, System.IO.SeekOrigin direction)
        {
            Log.WriteLine("Seek()");
            return _sink.Seek(offset, direction);
        }

        public override void SetLength(long length)
        {
            Log.WriteLine("SetLength()");
            _sink.SetLength(length);
        }

        public override void Close()
        {
            Log.WriteLine("Close()");
            skip_record_ = true;
            byte[] buf = Encoding.UTF8.GetBytes(buffer_.ToString());
            _sink.Write(buf, 0, buf.Length);
            _sink.Flush();
            _sink.Close();
        }

        public override void Flush()
        {
            Log.WriteLine("Flush()");
            //_sink.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            Log.WriteLine("Read()");
            return _sink.Read(buffer, offset, count);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            var ctx = HttpContext.Current;
            var query = (ctx.Request.Url.ToString() ?? "").ToLower();

            if (!tested_)
            {
                tested_ = true;
                if (ctx.Response.StatusCode == 200 && ctx.Response.ContentType.IndexOf("text/html") >= 0)
                {
                    should_record_ = true;
                    foreach (var key in ctx.Response.Headers.AllKeys)
                    {
                        response_headers_[key] = ctx.Response.Headers[key];
                    }
                }
            }
            if (should_record_ && !skip_record_)
            {
                string s = Encoding.UTF8.GetString(buffer, offset, count);
                Log.WriteLine("Write: " + offset.ToString() + " (" + count.ToString() + ") " + buffer.GetHashCode().ToString());

                buffer_.Append(s);
            }
            //_sink.Write(buffer, offset, count);
        }
    }
}
