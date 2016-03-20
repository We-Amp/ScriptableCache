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


namespace WeAmp.EasyCacheDropin
{
    public static class Log
    {
        static DateTime start = DateTime.Now;

        public static void WriteLine(string fmt, params string[] args)
        {
            string msg = String.Format(fmt, args);
            msg = String.Format("easycache {0}: {1}", (DateTime.Now - start).TotalMilliseconds, msg);
            Debug.WriteLine(msg);
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
 
            lru_ = new LurchTable<string, CacheItem>(100000);
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
        public List<byte> body { get; set; }
        public NameValueCollection response_headers { get; set; }
    }

    public class ConfigEntry
    {
        public int TTL;
    }

    public class EasyCacheHttpModule : IHttpModule
    {
        MemoryCache cache_ = null;
        FileSystemWatcher config_watcher_ = null;
        public static object configLock = new object();
        public static System.Type script_ = null;

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
            Log.WriteLine("App_Error fired: " + HttpContext.Current.Server.GetLastError().ToString());

            // TODO: bad assumption. Should store in context somewhere.
            if (f is RecordingFilter)
            {
                ((RecordingFilter)f).Fail();
            }
        }

        private void App_EndRequest(object sender, EventArgs e)
        {
            //Log.WriteLine("App_EndRequest fired");

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
            if (e.FullPath.IndexOf("easycache.config")>= 0)
                ReloadConfig();
        }

        private void ReloadConfig()
        {
            Log.WriteLine("ReloadConfig()");

            string path = Path.Combine(HttpRuntime.AppDomainAppPath, "easycache.config");
            try
            {
                string[] lines = File.ReadAllLines(path);
                var provider = CSharpCodeProvider.CreateProvider("c#");
                var options = new CompilerParameters();
                var ass = new System.Uri(Assembly.GetExecutingAssembly().EscapedCodeBase).LocalPath;
                options.ReferencedAssemblies.Add("System.Web.dll");
                options.ReferencedAssemblies.Add(ass);
                string scripttemplate = @"
using System.Web;

public class DynamicRules
{{
public static int onrequest() {{
    HttpRequest request = HttpContext.Current.Request;
    {0}
}}

public static bool onresponse() {{
    HttpRequest request = HttpContext.Current.Request;
    HttpResponse response = HttpContext.Current.Response;
    {1}
}}

}}";
                string srq = "";
                string srs = "";
                bool in_resp = false;
                foreach (string line in lines)
                {
                    string sline = line.Trim();
                    if (sline == "onrequest():")
                    {
                        continue;
                    }
                    else if (sline == "onresponse():")
                    {
                        in_resp = true;
                        continue;
                    }
                    if (in_resp)
                    {
                        srs += line + System.Environment.NewLine;
                    }
                    else
                    {
                        srq += line + System.Environment.NewLine;
                    }
                }
                string replacedScript = String.Format(scripttemplate, srq, srs);
                var results = provider.CompileAssemblyFromSource(options, new[] { replacedScript });
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
                    System.Type t = results.CompiledAssembly.GetType("DynamicRules");
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
                Log.WriteLine("Decline request, no configuration for request [{0}]", query);
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
                EasyCacheResponseEntry easy_cache_entry = (EasyCacheResponseEntry)i.Value;
                if (easy_cache_entry.status == 200) {
                    Log.WriteLine("Serve cached response for [{0}]", query);
                    foreach (var key in easy_cache_entry.response_headers.AllKeys)
                    {
                        ctx.Response.Headers[key] = easy_cache_entry.response_headers[key];
                    }
                    ctx.Response.Headers["X-Cache"] = "HIT";
                    ctx.Response.Headers["X-Age"] = Math.Round((DateTime.Now - i.DateAdded).TotalSeconds).ToString();
                    ctx.Response.BinaryWrite(easy_cache_entry.body.ToArray());
                    HttpContext.Current.ApplicationInstance.CompleteRequest();
                } else
                {
                    Log.WriteLine("Rember recently failed or skipped recording for [{0}]", query);
                }
            }
            else
            {
                if (ctx.Request.HttpMethod != "GET"
                    || ctx.Request.Headers["if-modified-since"] != null
                    || ctx.Request.Headers["if-none-match"] != null)
                {
                    Log.WriteLine("Decline request on default request preconditions for [{0}]", query);
                    return;
                }
                Log.WriteLine("Recording response for [{0}]", query);
                var f = new RecordingFilter(ctx.Response.Filter, cache_, query, cfg);
                ctx.Response.Filter = f;
                ctx.Response.Headers["X-Cache"] = "MISS";
            }
        }

        private ConfigEntry GetConfigEntryForQuery(string query)
        {
            if (script_ == null) return null;

            lock(configLock)
            { 
                var r = (int)script_.GetMethod("onrequest").Invoke(null, null);
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
            if (should_record_)
            {
                if (!failed_)
                {
                    CacheItem i = new CacheItem(key_);
                    EasyCacheResponseEntry e = new EasyCacheResponseEntry();
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
                    buffer_ = null;
                } else
                {
                    FilterLog("Mark recording as failed in cache because failed_ is set");

                    CacheItem i = new CacheItem(key_);
                    EasyCacheResponseEntry e = new EasyCacheResponseEntry();
                    e.status = 501;
                    i.Value = e;

                    CacheItemPolicy p = new CacheItemPolicy();
                    p.AbsoluteExpiration = DateTime.Now.Add(new TimeSpan(0, 0, 300));
                    cache_.Add(i, p);
                }
            } else if (tested_) {
                FilterLog("Mark recording as failed in cache because should_record_ is not set based on response headers");
                CacheItem i = new CacheItem(key_);
                EasyCacheResponseEntry e = new EasyCacheResponseEntry();
                e.status = 502;
                i.Value = e;
                CacheItemPolicy p = new CacheItemPolicy();
                p.AbsoluteExpiration = DateTime.Now.Add(new TimeSpan(0, 0, 300));
                cache_.Add(i, p);
            } else
            {
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

                lock (EasyCacheHttpModule.configLock)
                {
                    bool script_cacheable = (bool)EasyCacheHttpModule.script_.GetMethod("onresponse").Invoke(null, null);
                    FilterLog(string.Format("Scripted cachability: {0}, Response status: {1}", script_cacheable.ToString(), ctx.Response.StatusCode.ToString()));
                    should_record_ = script_cacheable 
                        && ctx.Response.StatusCode == 200;
                }

                if (should_record_)
                {
                    foreach (var key in ctx.Response.Headers.AllKeys)
                    {
                        response_headers_[key] = ctx.Response.Headers[key];
                    }
                    string ce = response_headers_["content-encoding"] ?? "";
                    if (string.IsNullOrEmpty(ce))
                        ce = "none";
                    key_ = "enc-" + ce + ":" + key_;

                }
            }
            if (should_record_ && !skip_record_)
            {
                string s = Encoding.UTF8.GetString(buffer, offset, count);
                FilterLog("Write: " + offset.ToString() + " (" + count.ToString() + ") " + buffer.GetHashCode().ToString());
                int written = 0;
                for (int i = offset; written < count; i++, written++) {
                    buffer_.Add(buffer[i]);
                }
            }
            //_sink.Write(buffer, offset, count);
        }
    }
}
