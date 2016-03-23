using System;
using System.Diagnostics;
using System.Web;

// Module looks for the ScriptableCacheLogic class
public class ScriptableCacheLogic
{
    // Module expects ScriptableCacheLogic::OnRequest to exist
    public static int OnRequest()
    {
        HttpRequest request = HttpContext.Current.Request;
        if (request.HttpMethod == "GET")
        {
            return 100;
        }
        return 0;
    }

    // Module expects ScriptableCacheLogic::OnResponse to exist
    public static bool OnResponse() 
    {
        HttpRequest request = HttpContext.Current.Request;
        HttpResponse response = HttpContext.Current.Response;
        return response.ContentType.IndexOf("text/html") >= 0;
    }
}

 