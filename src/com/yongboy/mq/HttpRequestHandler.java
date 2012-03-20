package com.yongboy.mq;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.jboss.netty.util.CharsetUtil;

/**
 * 处理简单的队列HTTP请求
 * 
 * @author yongboy
 * @time 2012-3-19
 * @version 1.0
 */
public class HttpRequestHandler extends SimpleChannelUpstreamHandler {
	private static final String RESPONSE_TEMPLATE = "{s:%d, m:'%s'}";

	private QueueService queueService;

	public HttpRequestHandler(QueueService queueService) {
		this.queueService = queueService;
	}

	private String decodeUri(String uri) {
		try {
			return URLDecoder.decode(uri, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			try {
				return URLDecoder.decode(uri, "ISO-8859-1");
			} catch (UnsupportedEncodingException e1) {
				return uri;
			}
		}
	}

	private final static String PATTERN = "([^?|!]*)(.*)";

	/**
	 * 从URL中抽取队列名
	 * 
	 * @author yongboy
	 * @time 2012-3-20
	 * 
	 * @param startPrefix
	 * @param uri
	 * @return
	 */
	private static String getParameterFromUri(String startPrefix, String uri) {
		Pattern pattern = Pattern.compile(startPrefix + PATTERN,
				Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(uri);
		if (!matcher.find())
			return null;

		return matcher.group(1);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		HttpRequest request = (HttpRequest) e.getMessage();
		String uri = decodeUri(request.getUri());
		QueryStringDecoder queryStringDecoder = new QueryStringDecoder(
				request.getUri());
		Map<String, List<String>> params = queryStringDecoder.getParameters();

		String queueName = null;
		String message = null;
		String optName = null;
		if (uri.startsWith("/get/")) {
			optName = "get";
			queueName = getParameterFromUri("/get/", uri);
		} else if (uri.startsWith("/put/")) {
			optName = "put";
			queueName = getParameterFromUri("/put/", uri);
		} else {
			if (params.isEmpty()) {
				writeResponse(
						e,
						OK,
						String.format(RESPONSE_TEMPLATE, 0, "miss parameters!"),
						request);
				return;
			}

			queueName = getParameterValue("name", params);
			optName = getParameterValue("opt", params);
		}

		if (optName == null || optName.trim().equals("")) {
			optName = "get";
		}

		if (queueName == null || queueName.trim().equals("")) {
			writeResponse(e, OK,
					String.format(RESPONSE_TEMPLATE, 0, "miss queue name!"),
					request);
			return;
		}

		if (optName.toLowerCase().equals("get")) {
			String result = queueService.pop(queueName);
			if (result == null) {
				result = "";
			}

			writeResponse(e, OK, String.format(RESPONSE_TEMPLATE, 1, result),
					request);
			return;
		}

		message = getParameterValue("msg", params);

		if (message == null) {
			writeResponse(e, OK,
					String.format(RESPONSE_TEMPLATE, 0, "miss parameter msg!"),
					request);
			return;
		}

		// 执行插入队列操作
		long result = queueService.push(queueName, message);

		writeResponse(e, OK, String.format(RESPONSE_TEMPLATE, 1, result),
				request);
	}

	private String getParameterValue(String parameterName,
			Map<String, List<String>> params) {
		List<String> values = params.get(parameterName);
		String parameterValue = null;
		if (values != null && !values.isEmpty()) {
			parameterValue = values.get(0);
		}

		return parameterValue;
	}

	private void writeResponse(MessageEvent e,
			HttpResponseStatus httpResponseStatus, String bufString,
			HttpRequest request) {
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1,
				httpResponseStatus);
		response.setContent(ChannelBuffers.copiedBuffer(bufString,
				CharsetUtil.UTF_8));
		response.setHeader(CONTENT_TYPE, "application/json; charset=UTF-8");

		ChannelFuture future = e.getChannel().write(response);

		future.addListener(ChannelFutureListener.CLOSE);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		e.getCause().printStackTrace();
		e.getChannel().close();
	}
}