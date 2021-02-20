package com.corgi;

import com.sun.tools.javac.util.List;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

class CorgiWorkerApplicationTests {

    @Test
    void contextLoads() {
        int[] people = new int[100];
        for (int i = 0; i < 100; i++) {
            people[i] = 100;
        }
        Random random = new Random();
        for (int j = 0; j < 10000000; j++) {
            for (int i = 0; i < 100; i++) {
                if (people[i] == 0) {
                    continue;
                }
                people[i]--;
                int index = random.nextInt(100);
                people[index]++;
            }
        }
        ArrayList<Integer> result = new ArrayList<Integer>();
        for (int i = 0; i < 100; i++) {
            result.add(people[i] + 1);
        }
        result.sort(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1 - o2;
            }
        });
        for (int i = 1; i <= 100; i++) {
            System.out.println(i);
        }
        System.out.println(result);
    }

}
