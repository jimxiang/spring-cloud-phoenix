package com.service.phoenix.aspect;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.*;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Aspect
@Component
public class ApiControllerAop {

    private static final Logger logger = LoggerFactory.getLogger(ApiControllerAop.class);

    /**
     * 指定切点
     * 匹配 com.example.demo.controller包及其子包下的所有类的所有方法
     */
    @Pointcut("execution(public * com.service.phoenix.controller.*.*(..))")
    public void webLog() {
    }

    private ThreadLocal<Long> startTime = new ThreadLocal<>();

    /**
     * 前置通知，方法调用前被调用
     *
     * @param joinPoint:
     */
    @Before("webLog()")
    public void doBefore(JoinPoint joinPoint) {
        startTime.set(System.currentTimeMillis());

        Signature signature = joinPoint.getSignature();
        logger.info("SIGNATURE: " + signature.getDeclaringTypeName());

        MethodSignature methodSignature = (MethodSignature) signature;
        String[] parameterNames = methodSignature.getParameterNames();
        Object[] args = joinPoint.getArgs();
        Map<String, Object> paramsMap = new HashMap<>();
        for (int i = 0; i < parameterNames.length; i++) {
            paramsMap.put(parameterNames[i], args[i]);
        }
        logger.info("PARAMS : " + paramsMap.toString());

        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        HttpServletRequest req = Objects.requireNonNull(attributes).getRequest();

        String message = "REQUEST_URL=" + req.getRequestURL().toString() + ", " +
                "HTTP_METHOD=" + req.getMethod() + ", " +
                "IP=" + req.getRemoteAddr() + ", " +
                "CLASS_METHOD=" + joinPoint.getSignature().getDeclaringTypeName() + "." + joinPoint.getSignature().getName();
        logger.info(message);

    }

    /**
     * 处理完请求返回内容
     *
     * @param ret:
     */
    @AfterReturning(returning = "ret", pointcut = "webLog()")
    public void doAfterReturning(Object ret) {
        logger.info("RETURN TIME: " + (System.currentTimeMillis() - startTime.get()));
    }
}
