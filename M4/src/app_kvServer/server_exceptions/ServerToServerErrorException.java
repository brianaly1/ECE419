package server_exceptions;

import java.lang.Exception;

public class ServerToServerErrorException extends Exception 
{ 
    public ServerToServerErrorException(String s) 
    { 
        super(s); 
    } 
} 