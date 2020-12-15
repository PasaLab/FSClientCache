package alluxio.client.file.cache.algo.cache;

public class UnitLoader implements Loader<UnitIndex, Unit> {

    @Override
    public Unit load(UnitIndex key) {
        return new Unit(key, null);
    }
}
