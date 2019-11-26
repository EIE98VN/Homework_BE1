package vn.edu.topica.eco.epayment.scheduler;

import java.util.*;

import com.github.icovn.http.client.HttpMethod;
import com.github.icovn.http.client.HttpResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import vn.edu.topica.eco.epayment.constant.PaymentStatus;
import vn.edu.topica.eco.epayment.exception.RecoverEx;
import vn.edu.topica.eco.epayment.model.EcomOrder;
import vn.edu.topica.eco.epayment.model.support.*;
import vn.edu.topica.eco.epayment.service.*;
import vn.edu.topica.eco.epayment.util.ErrorLogger;

import static vn.edu.topica.eco.epayment.constant.ErrorMessage.API_ACTIVATE_CODE;
import static vn.edu.topica.eco.epayment.constant.ErrorMessage.API_CANCEL_COURSE;
import static vn.edu.topica.eco.epayment.constant.ErrorType.EXCEPTION;

/**
 * This class is used to synchronize timeout transaction
 *
 * <p>author: huynv5
 */
@Component
@Slf4j
public class ScheduledEcomTransaction {

  @Autowired EcomTransactionService ecomTransactionService;

  @Autowired SyncContactService syncContactService;

  @Autowired MarolService marolService;

  @Autowired MagentoService magentoService;

  @Autowired EcomCodeService ecomCodeService;

  @Value("${spring.api.cancel.course}")
  private String cancelCourseUrl;

  @Autowired HttpService httpService;

  /** Get all expired order (order in CREATED, PENDING status) to synchronize */
  @Scheduled(fixedDelayString = "900000") // time in milliseconds (15 min)
  private void updateExpiredTransaction() {
    try {
      // Order with more than 45 minutes of no-action taking is considered as expired order
      Date startTime = new DateTime().minusDays(1).minusMinutes(45).toDate();
      Date endTime = new DateTime().minusMinutes(45).toDate();
      log.info(
          "(updateExpiredTransaction) startUpdateTime: {} - endUpdateTime: {}", startTime, endTime);
      List<String> listStatus = Arrays.asList(PaymentStatus.CREATED, PaymentStatus.PENDING);

      // Get expired orders
      List<EcomTransaction> orderList =
          ecomTransactionService.findOrdersByStatusIn(startTime, endTime, listStatus);
      log.info("(updateExpiredTransaction) numberContact: {}", orderList.size());

      // Synchronize orders
      for (EcomTransaction transaction : transactionList) {
        syncOrderExpired(transaction);
      }

      log.info("(updateExpiredTransaction) END");
    } catch (Exception ex) {
      log.error("(updateExpiredTransaction) EX: {}", ex.getMessage());
    }
  }

  /**
   * Synchronize ecom transaction and create C3 contact
   *
   * @param transaction transaction in marketplace
   */
  private void syncExpiredTransaction(EcomTransaction transaction) {
    log.info("(syncExpiredTransaction) START, chargeId: {}", order.getChargeId());

    String originOrderStatus = order.getStatus();

    // step 1: sync transaction
    order.setStatus(PaymentStatus.TIMEOUT);
    order.setTimeOutAt(new Date());
    ecomTransactionService.save(order);

    // step 2: sync activation code and cancel course
    List<EcomCode> ecomCodeList = ecomCodeService.findByEcomTransactionId(transaction.getId());

    if (ecomCodeList == null) {
      log.info("ERROR_ECOM_CODE_NULL; ecomTransactionId: {}", order.getId());
      return;
    }

    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json;charset=UTF-8");

    ecomCodeList.forEach(
        ecomCode -> {
          Map<String, String> requestBody = new HashMap<>();
          requestBody.put("cod_code", ecomCode.getActivateCode());
          try {
            HttpResult httpResult =
                httpService.execute(
                    cancelCourseUrl, headers, requestBody, HttpMethod.POST, API_CANCEL_COURSE);

            if (httpResult.getStatusCode() == 200) {
              ecomCode.setStatus(PaymentStatus.FAILED);
            }
            ecomCodeService.save(ecomCode);

          } catch (RecoverEx recoverEx) {
            ErrorLogger.pushException(EXCEPTION, API_ACTIVATE_CODE, recoverEx);
          }
        });

    // step 3: sync Bifrost with falied status
    if (originOrderStatus != null && originOrderStatus.equals(PaymentStatus.PENDING)) {
      OmiseEventData expiredEvent = new OmiseEventData();
      expiredEvent.setId(transaction.getChargeId());
      expiredEvent.setStatus(PaymentStatus.FAILED);
      syncContactService.updateTransactionBifrost(expiredEvent);
    }

    // step 4: create Marol C3 contact
    OrderMagento orderMagento = magentoService.findOrderById(order.getOrderId());
    if (orderMagento == null) {
      log.error(
          "etl_internal_error= ERROR_CANNOT_GET_ORDER_MAGENTO; chargeId: {}", order.getChargeId());
      return;
    }
    UserMagento userMagento = magentoService.findUserById(order.getUserId());
    if (userMagento == null) {
      log.error(
          "etl_internal_error= ERROR_CANNOT_GET_USER_MAGENTO; chargeId: {}", order.getChargeId());
      return;
    }

    // if there exists user's phone, create C3
    if (!StringUtils.isBlank(userMagento.getPhone()))
      createC3convert(orderMagento, userMagento, transaction.getContactMethod());

    log.info("(syncExpiredTransaction) END, chargeId: {}", order.getChargeId());
  }

  /**
   * This method is used to create C3 with timeout status
   *
   * @param orderMagento order in marketplace
   * @param userMagento user in marketplace
   * @param method payment method
   */
  private void createC3convert(OrderMagento orderMagento, UserMagento userMagento, String method) {

    OmiseEventData expiredData = new OmiseEventData();
    expiredData.setId("");
    expiredData.setStatus(PaymentStatus.TIMEOUT);

    List<ItemMagento> itemMagentoList = orderMagento.getCourses();

    // create C3 contact then update C3 status
    itemMagentoList.forEach(
        itemMagento -> {
          String cid = marolService.importContactC3(userMagento, itemMagento, method);
          syncContactService.updateC3StatusInMarol(cid, expiredData);
        });
  }
}
