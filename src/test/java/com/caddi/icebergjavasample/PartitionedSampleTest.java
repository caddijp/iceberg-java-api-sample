package com.caddi.icebergjavasample;


import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.time.OffsetDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PartitionedSampleTest {

    /**
     * AWS_REGION が環境変数にない場合テストが失敗するので、存在しない場合は適当な値を設定してください。
     * `export AWS_REGION=us-west-2`
     */
    @Test
    void testDataInsert() throws Exception {
        var rnd = new SecureRandom();
        var namespace = "ptest" + rnd.nextInt(1000);
        var tableName = "ptable" + rnd.nextInt(1000);
        System.out.println("test table " + namespace + "." + tableName);
        var target = new PartitionedSample();
        var input = getClass().getResourceAsStream("/sample.jsonl");
        target.perform("http://localhost:8181", namespace, tableName, input, true);

        var act = TestHelper.getData(namespace, tableName);
        assertEquals(5, act.size());

        assertEquals(UUID.fromString("00000000-1111-2222-3333-000000000001"), act.getFirst().get("id"));
        assertEquals("test1", act.getFirst().get("name"));
        assertEquals(100, act.getFirst().get("price"));
        assertEquals(OffsetDateTime.parse("2025-04-01T00:00:00.000000+00:00"), act.getFirst().get("registered_at"));
        
    }
}
