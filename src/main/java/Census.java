import java.io.Closeable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Implement the two methods below. We expect this class to be stateless and thread safe.
 */
public class Census {
    /**
     * Number of cores in the current machine.
     */
    private static final int CORES = Runtime.getRuntime().availableProcessors();

    /**
     * Output format expected by our tests.
     */
    public static final String OUTPUT_FORMAT = "%d:%d=%d"; // Position:Age=Total

    /**
     * Factory for iterators.
     */
    private final Function<String, Census.AgeInputIterator> iteratorFactory;

    /**
     * Creates a new Census calculator.
     *
     * @param iteratorFactory factory for the iterators.
     */
    public Census(Function<String, Census.AgeInputIterator> iteratorFactory) {
        this.iteratorFactory = iteratorFactory;
    }

    /**
     * Given one region name, call {@link #iteratorFactory} to get an iterator for this region and return
     * the 3 most common ages in the format specified by {@link #OUTPUT_FORMAT}.
     */
    public String[] top3Ages(String region) {

//        In the example below, the top three are ages 10, 15 and 12
//        return new String[]{
//                String.format(OUTPUT_FORMAT, 1, 10, 38),
//                String.format(OUTPUT_FORMAT, 2, 15, 35),
//                String.format(OUTPUT_FORMAT, 3, 12, 30) };
        Map<Integer, Long> ageCount = new HashMap<>();
        try (AgeInputIterator iterator = iteratorFactory.apply(region)){
            while (iterator.hasNext()) {
                Integer age = iterator.next();
                if (age < 0) continue;
                ageCount.merge(age, 1L, Long::sum);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        int expectedCount = Math.min(4, ageCount.size());
        return formatTopThree(ageCount, expectedCount);
    }

    private Map<Integer, Integer> getAgeCounts(AgeInputIterator iterator) {
        Map<Integer, Integer> ageCount = new HashMap<>();
        while (iterator.hasNext()) {
            Integer age = iterator.next();
            if (age > 0) {
                ageCount.put(age, ageCount.getOrDefault(age, 0) + 1);
            }
        }
        return ageCount;
    }

    private String[] formatTopThree(Map<Integer, Long> ageCount, int n) {
        AtomicInteger rank = new AtomicInteger(1);
        int[] lastRank = {0};
        long[] lastValue = {Long.MIN_VALUE};

        return ageCount.entrySet()
                .stream()
                .sorted((entry1, entry2) -> {
                    int countComparison = Long.compare(entry2.getValue(), entry1.getValue());
                    return countComparison != 0 ? countComparison : Integer.compare(entry1.getKey(), entry2.getKey());
                })
                .limit(n)
                .map(entry -> {
                    if (entry.getValue() != lastValue[0]){
                        lastRank[0] = rank.get();
                        lastValue[0] = entry.getValue();
                        rank.incrementAndGet();
                }
                    if (lastRank[0] > 3) {
                        return null;
                    }

                    return String.format(OUTPUT_FORMAT,
                            lastRank[0],
                            entry.getKey(),
                            entry.getValue());
                })
                .filter(result -> result != null)
                .toArray(String[]::new);
    }

    /**
     * Given a list of region names, call {@link #iteratorFactory} to get an iterator for each region and return
     * the 3 most common ages across all regions in the format specified by {@link #OUTPUT_FORMAT}.
     * We expect you to make use of all cores in the machine, specified by {@link #CORES).
     */
    public String[] top3Ages(List<String> regionNames) {

//        In the example below, the top three are ages 10, 15 and 12
//        return new String[]{
//                String.format(OUTPUT_FORMAT, 1, 10, 38),
//                String.format(OUTPUT_FORMAT, 2, 15, 35),
//                String.format(OUTPUT_FORMAT, 3, 12, 30) };
        Map<Integer, Long> globalAgeCount = new ConcurrentHashMap<>();

        regionNames.parallelStream().forEach(region -> {
            Map<Integer, Long> regionAgeCount = new HashMap<>();

            try (AgeInputIterator iterator = iteratorFactory.apply(region)){
                while (iterator.hasNext()) {
                    Integer age = iterator.next();
                    if (age < 0) continue;
                    regionAgeCount.merge(age, 1L, Long::sum);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            synchronized (globalAgeCount) {
                regionAgeCount.forEach((age, count) ->
                        globalAgeCount.merge(age, count, Long::sum));
            }
        });

        int expectedCount = Math.min(4, globalAgeCount.size());
        return formatTopThree(globalAgeCount, expectedCount);
    }


    /**
     * Implementations of this interface will return ages on call to {@link Iterator#next()}. They may open resources
     * when being instantiated created.
     */
    public interface AgeInputIterator extends Iterator<Integer>, Closeable {
    }
}
