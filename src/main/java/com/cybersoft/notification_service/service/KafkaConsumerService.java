package com.cybersoft.notification_service.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
    @Autowired
    private JavaMailSender mailSender;

    @Value("${spring.mail.username}")
    private String serverEmail;

    // Lắng nghe hòm thư "user-registration-topic"
    @KafkaListener(topics = "user-registration-email", groupId = "group-email")
    public void listenRegistrationEvent(String userEmail) {
        System.out.println("[Consumer] Đang chuẩn bị gửi mail cho: " + userEmail);

        try {
            SimpleMailMessage message = new SimpleMailMessage();
            message.setFrom(serverEmail);
            message.setTo(userEmail);
            message.setSubject("Đăng ký tài khoản thành công");
            message.setText("Chào mừng bạn đến với hệ thống của chúng tôi. Chúc bạn một ngày tốt lành");

            mailSender.send(message);
            System.out.println("[Consumer] Gửi thành công tới: " + userEmail);

        } catch (Exception e) {
            System.err.println("[Consumer] Lỗi khi gửi mail: " + e.getMessage());
        }
    }
}
