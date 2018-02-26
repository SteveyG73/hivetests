import com.google.common.collect.Sets;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

@RunWith(StandaloneHiveRunner.class)
public class HiveStatementTest {

    @HiveSQL(files = { },autoStart = false)
    private HiveShell shell;

    @Before
    public void setupSourceDatabase() {
        /*
        File srcData = new File("src/test/resources/test_orders_ext_hist.csv");
        File target = new File("/tmp/unit_testing/load_staging/wpg/all/history/ORDERS_SQOOP_HIST_EXT/test_orders_ext_hist.csv");
        try {
            if (!target.exists()) {
                target.getParentFile().mkdirs();
                target.createNewFile();
            }
            Files.copy(srcData.toPath(),target.toPath(),StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        shell.setHiveConfValue("database","unit_testing");
        shell.addResource("tmp/sourcedata",srcData);
        */

        shell.setHiveConfValue("database","unit_testing");
        shell.start();
        shell.execute(Paths.get("src/test/resources/create_unit_testing_db.hql"));
    }

    @Test
    public void testTableCreated() {

        HashSet<String> expected = Sets.newHashSet("sg_basic_text_table");
        shell.execute(Paths.get("src/test/resources/create_simple_table.hql"));
        HashSet<String> actual = Sets.newHashSet(shell.executeQuery("SHOW TABLES"));

        Assert.assertEquals(expected, actual);


    }

    @Test
    public void selectSomeData() {
        int expected = 1;
        shell.execute(Paths.get("src/test/resources/create_simple_table.hql"));
        shell.insertInto("unit_testing","sg_basic_text_table")
                .addRow(1,"row1","The first row")
                .commit();
        List<String> actual = shell.executeQuery("SELECT * FROM ${hiveconf:database}.sg_basic_text_table");
        Assert.assertEquals(expected,actual.size());
    }
}
