

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@page import="org.example.www.AstyanaxClass"%>
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>JSP Page</title>
    </head>
    <body>
        <h1>Hello World!</h1>
        <% AstyanaxClass contextObj = new AstyanaxClass(); %>
        <%//= contextObj.createKS() %>
        <%//= contextObj.createCF() %>
        <%//= contextObj.insertData() %>
        
        
        <%= contextObj.retreiveData() %>
        
    </body>
</html>
