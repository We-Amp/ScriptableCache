using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Text;
using System.Web;
using CSharpTest.Net.Collections;
using System.Diagnostics;
using Microsoft.CSharp;
using System.CodeDom.Compiler;
using System.Reflection;


namespace WeAmp.ScriptableCacheModule
{
    public static class Log
    {
        static DateTime start = DateTime.Now;
        static CircularBuffer<string> Buffer = new CircularBuffer<string>(1000);
        public static void WriteLine(string fmt, params string[] args)
        {
            string msg = String.Format(fmt, args);
            msg = String.Format("[WeAmp.ScriptableCache][{0}] {1}", DateTime.Now.ToLongTimeString(), msg);
            Buffer.Put(msg);
            Trace.WriteLine(msg);
        }
        public static string GetLog()
        {
            StringBuilder sb = new StringBuilder();
            string[] lines = Buffer.ToArray();
            for (int i = lines.Length - 1; i >= 0; --i)
            {
                sb.AppendLine(lines[i]);
            }
            return sb.ToString();
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
        static CSharpTest.Net.Collections.LurchTable<string, CacheItem> lru_;

        public static int GetApproxItemCount()
        {
            lru_.ItemRemoved += Lru__ItemRemoved;
            return lru_.Count;
        }

        private static void Lru__ItemRemoved(KeyValuePair<string, CacheItem> obj)
        {
            ++Statistics.CacheItemsEvicted;
            Log.WriteLine("Cache entry evicted: " + obj.Value.Key + "(" + ((ScriptableCacheResponseEntry)obj.Value.Value).status.ToString() + ")");
        }

        public MemoryCache(string name)
        {
 
            lru_ = new LurchTable<string, CacheItem>(LurchTableOrder.Access, 10000);
        }
        public void Add(CacheItem i, CacheItemPolicy p)
        {
            i.ValidUntil = p.AbsoluteExpiration;
            i.DateAdded = DateTime.Now;
            bool success = lru_.TryAdd(i.Key, i);
            
            Log.WriteLine("Add [{0}] => {1} (cache has {2} items): ", i.Key, success.ToString(), lru_.Count.ToString());
        }
        public CacheItem GetCacheItem(string key)
        {
            CacheItem v;
            if (lru_.TryGetValue(key, out v))
            {
                if (DateTime.Now >= v.ValidUntil)
                {
                    Log.WriteLine("Found [{0}] but {1} >= {2}", key, DateTime.Now.ToString(), v.ValidUntil.ToString());
                    if (lru_.TryRemove(key, out v))
                    {
                        Log.WriteLine("Eviction failed for [{0}] but {1} >= {2}", key, DateTime.Now.ToString(), v.ValidUntil.ToString());
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

    class ScriptableCacheResponseEntry
    {
        public int status;
        public List<byte> body { get; set; }
        public NameValueCollection response_headers { get; set; }
    }

    public class ConfigEntry
    {
        public int TTL;
    }


    public static class Statistics
    {
        internal static int Hits = 0;
        internal static int Misses = 0;
        internal static int Uncacheable = 0;
        internal static int AppErrorsFired = 0;
        internal static int ConfigReloads = 0;
        internal static int TotalRequests;
        internal static int PurgesProcessed;
        internal static int OnRequestExcepted;
        internal static int FailedRecordings;
        internal static int DeclinedRecordings;
        internal static int AbortedRecordings;
        internal static int CacheItemsAdded;
        internal static int OnResponseExcepted;
        internal static int RecordingsStarted;
        internal static int CacheItemsEvicted;
    }

    public class ScriptableCacheHttpModule : IHttpModule
    {
        static MemoryCache cache_ = null;
        static FileSystemWatcher config_watcher_ = null;
        public static object configLock = new object();
        public static System.Type script_ = null;

        public void Dispose()
        {

        }
        public void Init(HttpApplication app)
        {
            Log.WriteLine("Application init fired");
            app.BeginRequest += App_BeginRequest;
            app.EndRequest += App_EndRequest;
            app.Error += App_Error;

            lock (configLock) {
                if (cache_ != null)
                {
                    Log.WriteLine("Already initialized here, bail");
                    return;
                }
                cache_ = new MemoryCache("ScriptableCache");
                config_watcher_ = new FileSystemWatcher();
                config_watcher_.BeginInit();
                config_watcher_.Path = HttpRuntime.AppDomainAppPath;
                config_watcher_.IncludeSubdirectories = false;
                //config_watcher_.Filter = "*.cs";
                config_watcher_.Changed += Config_watcher__Changed;
                config_watcher_.EnableRaisingEvents = true;
                config_watcher_.EndInit();
                ReloadConfig();
            }
        }

        private void App_Error(object sender, EventArgs e)
        {
            ++Statistics.AppErrorsFired;
            Log.WriteLine("App_Error fired: " + HttpContext.Current.Server.GetLastError().ToString());
            var f = (Stream)HttpContext.Current.Items["WeAmp.ScriptCache.Filter"];
            if (f != null)
            {
                ((RecordingFilter)f).Fail();
            }
        }

        private void App_EndRequest(object sender, EventArgs e)
        {
    //        ++Statistics.AppErrorsFired;

            var f = (Stream)HttpContext.Current.Items["WeAmp.ScriptCache.Filter"];
            if (f != null)
            {
                var requestStart = (DateTime)HttpContext.Current.Items["WeAmp.ScriptCache.RequestStart"];
                Log.WriteLine("App_EndRequest fired in {0} ms", (DateTime.Now - requestStart).TotalMilliseconds.ToString());
                ((RecordingFilter)f).Done();
            }
        }

        private void Config_watcher__Changed(object sender, FileSystemEventArgs e)
        {
            if (e.FullPath.IndexOf("WeAmp.ScriptableCache.cs") >= 0) {
                ++Statistics.ConfigReloads;
                Log.WriteLine("Config_watcher__Changed fired {0} / {1}", e.FullPath, e.ChangeType.ToString());
                System.Threading.ThreadStart ts = new System.Threading.ThreadStart(DelayedReloadConfig);
                System.Threading.Thread t = new System.Threading.Thread(ts);
                t.IsBackground=true;
                t.Start();
            }
        }

        private void DelayedReloadConfig()
        {
            System.Threading.Thread.Sleep(250);
            ReloadConfig();
        }

        private void ReloadConfig()
        {
            Log.WriteLine("ReloadConfig()");

            string path = Path.Combine(HttpRuntime.AppDomainAppPath, "WeAmp.ScriptableCache.cs");
            try
            {
                string script = File.ReadAllText(path);
                var provider = CSharpCodeProvider.CreateProvider("c#");
                var options = new CompilerParameters();
                options.IncludeDebugInformation = true;
                var ass = new System.Uri(Assembly.GetExecutingAssembly().EscapedCodeBase).LocalPath;
                options.ReferencedAssemblies.Add("System.Web.dll");
                options.ReferencedAssemblies.Add("System.Data.dll");
                options.ReferencedAssemblies.Add("System.dll");
                options.ReferencedAssemblies.Add(ass);
                var results = provider.CompileAssemblyFromSource(options, new[] { script });
                if (results.Errors.Count > 0)
                {
                    script_ = null;
                    foreach (var error in results.Errors)
                    {
                        Log.WriteLine("Error loading script: " + error);
                    }
                }
                else
                {
                    System.Type t = results.CompiledAssembly.GetType("ScriptableCacheLogic");
                    if (t == null)
                    {
                        Log.WriteLine("Could not find target class in script source");
                    }
                    else {
                        Log.WriteLine("Script loaded OK!");
                    }
                    lock (configLock)
                    {
                        script_ = t;
                    }
                }
            }
            catch(Exception ex)
            {
                Log.WriteLine("Exception reloading config: {0}", ex.Message);
                lock (configLock)
                {
                    script_ = null;
                }
            }
        }

        private void App_BeginRequest(object sender, EventArgs e)
        {
            ++Statistics.TotalRequests;

            var app = (HttpApplication)sender;
            var ctx = app.Context;
            var query = ctx.Request.Url.ToString();
            HttpContext.Current.Items.Add("WeAmp.ScriptCache.RequestStart", DateTime.Now);


            string template =

                @"
<html>
<body>
<h1>Weamp.ScriptableCache Statistics</h1>
<pre>
{0}
</pre>
<script type='text/javascript'>
setTimeout('document.reload()', 5);
</script>
</body>
</html>
";

            // TODO(oschaaf): fix / should be configurable.
            if (query.Contains("/WeAmp.ScriptCache.Logs"))
            {
                HttpContext.Current.Response.ContentType = "text/html";
                HttpContext.Current.Response.Write(string.Format(template, Log.GetLog()));
                HttpContext.Current.ApplicationInstance.CompleteRequest();
                return;
            } else if (query.Contains("/WeAmp.ScriptCache.Purge"))
            {
                Log.WriteLine("Executing purge");
                cache_.PurgeAll();

                HttpContext.Current.Response.ContentType = "text/html";
                HttpContext.Current.Response.Write(string.Format(template, "Cache purged OK"));
                ++Statistics.PurgesProcessed;
                HttpContext.Current.ApplicationInstance.CompleteRequest();
                return;
            } else 
            if (query.EndsWith("/Weamp.ScriptCache.Stats"))
            {

                string s = "";
                s += "Total Requests seen: " + Statistics.TotalRequests + "\n";
                s += "Cache Hits: " + Statistics.Hits + "\n";
                s += "Cache Misses: " + Statistics.Misses + "\n";
                s += "Entries currently in cache: " + MemoryCache.GetApproxItemCount().ToString() + "\n";
                s += "Total cache items added: " + Statistics.CacheItemsAdded + "\n";
                s += "Total cache items evicted: " + Statistics.CacheItemsEvicted + "\n";
                s += "Uncacheable requests seen: " + Statistics.Uncacheable + "\n";
                s += "Config Reloads: " + Statistics.ConfigReloads + "\n";
                s += "Cache Purge requests Processed: " + Statistics.PurgesProcessed + "\n";
                s += "Application error events fired: " + Statistics.AppErrorsFired + "\n";
                s += "OnRequest calls Excepted: " + Statistics.OnRequestExcepted + "\n";
                s += "OnResponse calls Excepted: " + Statistics.OnResponseExcepted + "\n";
                s += "Initiated cache recordings: " + Statistics.RecordingsStarted + "\n";
                s += "Failed cache recordings: " + Statistics.FailedRecordings + "\n";
                s += "Declined cache recordings: " + Statistics.DeclinedRecordings + "\n";
                s += "Aborted cache  recordings: " + Statistics.AbortedRecordings + "\n";
                HttpContext.Current.Response.ContentType = "text/html";
                HttpContext.Current.Response.Write(string.Format(template, s));
                HttpContext.Current.ApplicationInstance.CompleteRequest();
                return;
           }


            char[] ch = { ',' };
            List<string> ces = new List<string>();
            ces.AddRange((HttpContext.Current.Request.Headers["accept-encoding"] ?? "").Split(ch));
            ces.Add("");
            CacheItem i = null;
            Log.WriteLine("Accept-Encodings [{0}] for query [{1}]", (HttpContext.Current.Request.Headers["accept-encoding"] ?? ""), query);

            foreach (string enc in ces)
            {
                string senc = enc.Trim();
                if (string.IsNullOrEmpty(senc))
                {
                    senc = "none";
                }
                i = cache_.GetCacheItem("enc-" + senc + ":" + query);
                if (i != null)
                {
                    Log.WriteLine("Found cache entry! [{0}]", "enc-" + senc + ":" + query);
                    break;
                }
            }

            if (i != null)
            {
                ScriptableCacheResponseEntry cache_entry = (ScriptableCacheResponseEntry)i.Value;
                if (cache_entry.status == 200) {
                    ++Statistics.Hits;
                    foreach (var key in cache_entry.response_headers.AllKeys)
                    {
                        ctx.Response.Headers[key] = cache_entry.response_headers[key];
                    }
                    ctx.Response.Headers["X-Cache"] = "HIT";
                    ctx.Response.Headers["X-Age"] = Math.Round((DateTime.Now - i.DateAdded).TotalSeconds).ToString();
                    ctx.Response.BinaryWrite(cache_entry.body.ToArray());
                    var requestStart = (DateTime)HttpContext.Current.Items["WeAmp.ScriptCache.RequestStart"];
                    Log.WriteLine("Handled cached request in {0} ms: [{1}]", (DateTime.Now - requestStart).TotalMilliseconds.ToString(), query);
                    HttpContext.Current.ApplicationInstance.CompleteRequest();
                    return;
                } else
                {
                    Log.WriteLine("Rember recently failed or skipped recording for [{0}]", query);
                }
            }
            else
            {
                ++Statistics.Misses;
                ConfigEntry cfg = GetConfigEntryForQuery(query);
                if (cfg == null)
                {
                    Log.WriteLine("Decline request, no configuration for request [{0}]", query);
                    return;
                }
                if (ctx.Request.HttpMethod != "GET"
                    || ctx.Request.Headers["if-modified-since"] != null
                    || ctx.Request.Headers["if-none-match"] != null)
                {
                    Log.WriteLine("Decline request on default request preconditions for [{0}]", query);
                    return;
                }
                Log.WriteLine("Recording response for [{0}]", query);
                ++Statistics.RecordingsStarted;
                var f = new RecordingFilter(ctx.Response.Filter, cache_, query, cfg);
                ctx.Response.Filter = f;
                HttpContext.Current.Items.Add("WeAmp.ScriptCache.Filter", f);

                ctx.Response.AddHeader("X-Cache", "Miss");
            }
        }

        private ConfigEntry GetConfigEntryForQuery(string query)
        {
            if (script_ == null)
            {
                Log.WriteLine("script_ == null, return null");
                return null;
            }
            lock(configLock)
            {
                int r=-99999999;
                try { 
                    r = (int)script_.GetMethod("OnRequest").Invoke(null, null);
                    Log.WriteLine("OnRequest TTL: " + r.ToString());
                }
                catch (Exception ex)
                {
                    Log.WriteLine("OnRequest excepted: " + ex.ToString());
                    ++Statistics.OnRequestExcepted;
                }

                if (r <= 0)
                    return null;
                return new ConfigEntry
                {
                    TTL = r
                };
            }
        }
    }

    public class RecordingFilter : Stream
    {
        private Stream _sink;
        List<byte> buffer_ = new List<byte>();
        MemoryCache cache_;
        string key_;
        NameValueCollection response_headers_;
        bool tested_;
        bool should_record_;
        bool failed_;
        bool skip_record_;
        ConfigEntry config_;

        public void FilterLog(string msg)
        {
            Log.WriteLine("{0} [{1}]", msg, key_);
        }

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
            string ce = response_headers_["content-encoding"] ?? "";
            if (string.IsNullOrEmpty(ce))
                ce = "none";
            key_ = "enc-" + ce + ":" + key_;

            if (should_record_)
            {
                if (!failed_)
                {
                    CacheItem i = new CacheItem(key_);
                    ScriptableCacheResponseEntry e = new ScriptableCacheResponseEntry();
                    e.status = 200;
                    e.body = buffer_;
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
                    FilterLog(string.Format("Add to cache, TTL: {0}, valid through: {1}", config_.TTL, p.AbsoluteExpiration.ToString()));
                    cache_.Add(i, p);
                    ++Statistics.CacheItemsAdded;
                    buffer_ = null;
                } else
                {
                    if (tested_) { 
                        FilterLog("Mark recording as failed in cache because failed_ is set");
                        ++Statistics.FailedRecordings;

                        CacheItem i = new CacheItem(key_);
                        ScriptableCacheResponseEntry e = new ScriptableCacheResponseEntry();
                        e.status = 501;
                        i.Value = e;

                        CacheItemPolicy p = new CacheItemPolicy();
                        p.AbsoluteExpiration = DateTime.Now.Add(new TimeSpan(0, 0, 300));
                        cache_.Add(i, p);
                    }
                }
            } else if (tested_) {
                ++Statistics.DeclinedRecordings;
                FilterLog("Mark recording as failed in cache because should_record_ is not set based on response headers");
                CacheItem i = new CacheItem(key_);
                ScriptableCacheResponseEntry e = new ScriptableCacheResponseEntry();
                e.status = 502;
                i.Value = e;
                CacheItemPolicy p = new CacheItemPolicy();
                p.AbsoluteExpiration = DateTime.Now.Add(new TimeSpan(0, 0, 300));
                cache_.Add(i, p);
            } else
            {
                ++Statistics.AbortedRecordings;
                FilterLog("Skip recording that was aborted early / not started");
            }
        }

        public void Fail()
        {
            FilterLog("Fail() called");
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
            FilterLog("Seek()");
            return _sink.Seek(offset, direction);
        }

        public override void SetLength(long length)
        {
            FilterLog("SetLength()");
            _sink.SetLength(length);
        }

        public override void Close()
        {
            FilterLog("Close()");
            skip_record_ = true;
            if (buffer_.Count> 0 ) { 
                _sink.Write(buffer_.ToArray(), 0, buffer_.Count);
            }
            _sink.Flush();
            _sink.Close();
        }

        public override void Flush()
        {
            FilterLog("Flush()");
            //_sink.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            FilterLog("Read()");
            return _sink.Read(buffer, offset, count);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            var ctx = HttpContext.Current;
            var query = (ctx.Request.Url.ToString() ?? "").ToLower();

            if (!tested_)
            {
                tested_ = true;

                lock (ScriptableCacheHttpModule.configLock)
                {
                    bool script_cacheable = false;
                    try {
                        script_cacheable = (bool)ScriptableCacheHttpModule.script_.GetMethod("OnResponse").Invoke(null, null);
                    }
                    catch(Exception ex)
                    {
                        ++Statistics.OnResponseExcepted;
                        script_cacheable = false;
                        FilterLog(string.Format("OnResponse call excepted: {0}", ex.ToString()));
                    }
                    FilterLog(string.Format("Scripted cachability: {0}, Response status: {1}", script_cacheable.ToString(), ctx.Response.StatusCode.ToString()));
                    should_record_ = script_cacheable 
                        && ctx.Response.StatusCode == 200;
                }

                if (should_record_)
                {
                    foreach (var key in ctx.Response.Headers.AllKeys)
                    {
                        // TODO(oschaaf): breaks in classic mode and pre .NET 3.5
                        // you can't seem to read headers over there.
                        response_headers_[key] = ctx.Response.Headers[key];
                    }
                }
            }
            if (should_record_ && !skip_record_)
            {
                FilterLog("Write: " + offset.ToString() + " (" + count.ToString() + ") " + buffer.GetHashCode().ToString());
                int written = 0;
                for (int i = offset; written < count; i++, written++) {
                    buffer_.Add(buffer[i]);
                }
            }
            if (!should_record_) {
                _sink.Write(buffer, offset, count);
            }
        }
    }
}
