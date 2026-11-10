package com.bank.risk.sim;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransactionFactoryTest {

    @Test
    void shouldBeDeterministicForGivenSeed() {
        TransactionFactory first = new TransactionFactory(42L);
        TransactionFactory second = new TransactionFactory(42L);

        for (int i = 0; i < 50; i++) {
            Transaction a = first.next(1730000000000L + i);
            Transaction b = second.next(1730000000000L + i);
            assertEquals(a.getTransactionId(), b.getTransactionId());
            assertEquals(a.getAmount(), b.getAmount());
            assertEquals(a.getMerchantId(), b.getMerchantId());
            assertEquals(a.getCity(), b.getCity());
        }
    }

    @Test
    void shouldProduceValidAndUniqueTransactions() {
        TransactionFactory factory = new TransactionFactory(20261110L);
        Set<String> ids = new HashSet<>();

        for (int i = 0; i < 500; i++) {
            Transaction tx = factory.next(1730000000000L + i);
            assertTrue(ids.add(tx.getTransactionId()), "流水号必须唯一");
            assertNotNull(tx.getUserId());
            assertNotNull(tx.getCity());
            assertNotNull(tx.getChannel());
            assertNotNull(tx.getCardNo());
            assertTrue(tx.getCardNo().length() >= 16);
            assertTrue(tx.getAmount().compareTo(BigDecimal.ZERO) > 0);
            assertEquals(2, tx.getAmount().scale());
            assertEquals("CNY", tx.getCurrency());
            assertEquals(1730000000000L + i, tx.getEventTime());
        }
        assertEquals(500, factory.generatedCount());
    }

    @Test
    void shouldInjectRiskSamples() {
        TransactionFactory factory = new TransactionFactory(7L);
        boolean hasLargeAmount = false;
        boolean hasBlacklistMerchant = false;

        for (int i = 0; i < 5000; i++) {
            Transaction tx = factory.next(1730000000000L + i);
            hasLargeAmount |= tx.getAmount().compareTo(new BigDecimal("50000")) >= 0;
            hasBlacklistMerchant |= "M-BLACK-001".equals(tx.getMerchantId());
        }

        assertTrue(hasLargeAmount, "应能生成大额交易样本");
        assertTrue(hasBlacklistMerchant, "应能生成黑名单商户样本");
    }
}
