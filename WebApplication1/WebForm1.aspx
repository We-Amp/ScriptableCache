<%@ Page Language="C#" AutoEventWireup="true" CodeBehind="WebForm1.aspx.cs" Inherits="WebApplication1.WebForm1" %>

<!DOCTYPE html>

<html xmlns="http://www.w3.org/1999/xhtml">
<head runat="server">
    <title></title>
</head>
<body>
    <form id="form1" runat="server">
    <div>
    fooo <%=DateTime.Now.ToString() %>
    </div>
        <%
          Response.Flush();
            System.Threading.Thread.Sleep(1000);
        %>
    </form>
</body>
</html>
