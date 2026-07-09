package io.tierdb.connector.seam;

import io.tierdb.common.mode.Mode;
import io.tierdb.connector.read.Cold;
import io.tierdb.connector.read.Read;
import io.tierdb.lake.access.LakeAccess;
import io.tierdb.lake.access.LakeScan;
import java.util.Optional;

public record SeamState(
        TableSeam table,
        LakeProfile lake,
        CutLine cutLine,
        Long pinId) {

    public Mode mode() {
        return table.mode();
    }

    public long readSeam() {
        return cutLine.readSeam();
    }

    public Read scan(LakeAccess access) {
        if (mode().heapComplete()) {
            return new Read.Heap();
        }
        long t = readSeam();
        if (mode().isDirect()) {
            return access.liveScan(lake().tableRef())
                    .<Read>map(scan -> new Read.Seam(t, new Cold.Live(scan)))
                    .orElseGet(Read.Heap::new);
        }
        Optional<LakeScan> pinned = access.pinnedScan(cutLine().lakeProps());
        Cold cold = pinned.<Cold>map(Cold.Merge::new).orElseGet(Cold.Delta::new);
        return new Read.Seam(t, cold);
    }

    public Read scanHybrid(LakeAccess access) {
        LakeScan scan = access.pinnedScan(cutLine().lakeProps())
                .orElseThrow(() -> new IllegalStateException(
                        "hybrid read requires pinned lake props on " + lake().tableRef()));
        return new Read.Seam(readSeam(), new Cold.Merge(scan));
    }
}
