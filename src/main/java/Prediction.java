
public record Prediction(
        Double officialDayMonthOfYearDate,
        Double officialTime,
        Integer street,
        Integer age,
        Integer ethnicity,
        Integer gender,
        Integer time,
        Integer plan,
        String total,
        Point point
        ) {

    public String getTotal() {
        return String.valueOf(street + age + ethnicity + gender + time + plan);
    }


}
