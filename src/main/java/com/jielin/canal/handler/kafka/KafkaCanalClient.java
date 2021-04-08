package com.jielin.canal.handler.kafka;

import cn.hutool.json.JSONUtil;
import com.jielin.canal.bean.Dml;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

/**
 * @author yxl
 * @description kafka消费client数据
 * @date 2021/3/30 1:14 下午
 * @since jdk1.8
 */

@Component
@Slf4j
public class KafkaCanalClient {

    @Autowired
    private SyncKafkaMessageHandler kafkaMessageHandler;

    @Autowired
    private JavaMailSender emailSender;

    @KafkaListener(topics = {"jl_db_rout"})
    public void process(String dmlStr, Acknowledgment ack) {
        Dml dml = JSONUtil.toBean(dmlStr, Dml.class);
        try {
            kafkaMessageHandler.handleMessage(dml);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Kafka消费binlog异常:", e);
            sendNormalText("xiaolong.yang@yueguanjia.com", "binlog消费异常", dmlStr, e);
        }
    }

    private void sendNormalText(String to, String subject, String parmaText, Exception e) {
        MimeMessage message = emailSender.createMimeMessage();
        try {
            MimeMessageHelper messageHelper = new MimeMessageHelper(message, true);
            messageHelper.setFrom("ping@yueguanjia.com");
            messageHelper.setTo(to);
            messageHelper.setSubject(subject);
            String html = "<div><h1><a name=\"binlog消费异常\"></a></h1><blockquote><p>" +
                    "<span><h2>binlog数据：</h2>" + parmaText + "</span><br/><br/>" +
                    "<span><h2>binlog消费异常原因：</h2>" + e + "</span></p></blockquote><p>&nbsp;</p><p><span></span></p></div>";
            messageHelper.setText(html, true);
            emailSender.send(message);
        } catch (MessagingException ex) {
            ex.printStackTrace();
        }
    }
}
