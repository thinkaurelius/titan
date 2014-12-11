package com.thinkaurelius.titan.hadoop.formats.util;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.schema.SchemaContainer;
import com.thinkaurelius.titan.hadoop.FaunusSchemaManager;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

public class TitanSchemaAwareMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

    private static final Logger log = LoggerFactory.getLogger(TitanSchemaAwareMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // Catch any exceptions, log a warning, and allow the subclass to continue even if schema loading failed
        try {
            ModifiableHadoopConfiguration faunusConf =
                    ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(context));

            if (faunusConf.get(TitanHadoopConfiguration.OUTPUT_TITAN_TYPE_CHECKING)) {
                TitanGraph g = TitanFactory.open(faunusConf.getOutputConf());
                FaunusSchemaManager.getTypeManager(null).setSchemaProvider(new SchemaContainer(g));
                log.info("Loaded schema associated with {}", g);
            } else {
                log.debug("Titan schema checking is disabled");
            }
        } catch (Throwable t) {
            log.warn("Unable to load Titan schema", t);
        }
    }
}
