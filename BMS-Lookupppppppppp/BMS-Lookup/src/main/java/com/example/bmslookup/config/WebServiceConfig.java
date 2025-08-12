package com.example.bmslookup.config;

import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.ws.config.annotation.EnableWs;
import org.springframework.ws.config.annotation.WsConfigurerAdapter;
import org.springframework.ws.server.EndpointInterceptor;
import org.springframework.ws.soap.server.endpoint.interceptor.PayloadValidatingInterceptor;
import org.springframework.ws.transport.http.MessageDispatcherServlet;
import org.springframework.ws.wsdl.wsdl11.DefaultWsdl11Definition;
import org.springframework.xml.xsd.SimpleXsdSchema;
import org.springframework.xml.xsd.XsdSchema;

import java.util.List;

@Configuration
@EnableWs
public class WebServiceConfig extends WsConfigurerAdapter {

  
    @Bean
    public ServletRegistrationBean<MessageDispatcherServlet> messageDispatcherServlet(ApplicationContext applicationContext) {
        MessageDispatcherServlet servlet = new MessageDispatcherServlet();
        servlet.setApplicationContext(applicationContext);
        servlet.setTransformWsdlLocations(true);
        
        return new ServletRegistrationBean<>(servlet, "/ws/*");
    }

 
    @Bean
    public XsdSchema uhiSchema() {
        return new SimpleXsdSchema(new ClassPathResource("uhi.xsd"));
    }


    @Bean(name = "service")
    public DefaultWsdl11Definition defaultWsdl11Definition(XsdSchema uhiSchema) {
        DefaultWsdl11Definition wsdl11Definition = new DefaultWsdl11Definition();
        

        wsdl11Definition.setSchema(uhiSchema);
        

        wsdl11Definition.setPortTypeName("UHIPort");

        wsdl11Definition.setLocationUri("/ws");
        

        wsdl11Definition.setTargetNamespace("http://teradata.com/uhi");
        
        return wsdl11Definition;
    }

   
    @Bean
    public PayloadValidatingInterceptor payloadValidatingInterceptor() {
        PayloadValidatingInterceptor interceptor = new PayloadValidatingInterceptor();
        interceptor.setXsdSchema(uhiSchema());
        interceptor.setValidateRequest(true);
        interceptor.setValidateResponse(true);
        interceptor.setAddValidationErrorDetail(true);
        return interceptor;
    }

    /**
     * تسجيل Interceptors
     */
    @Override
    public void addInterceptors(List<EndpointInterceptor> interceptors) {
        // Temporarily disabled XSD validation to fix startup issue
        // interceptors.add(payloadValidatingInterceptor());
    }

 
    @Bean
    public DefaultWsdl11Definition enhancedWsdl11Definition(XsdSchema uhiSchema) {
        DefaultWsdl11Definition wsdl11Definition = defaultWsdl11Definition(uhiSchema);
        
     
        wsdl11Definition.setServiceName("UHI Service");
        

        wsdl11Definition.setCreateSoap11Binding(true);
        wsdl11Definition.setCreateSoap12Binding(false);
        
        return wsdl11Definition;
    }
}