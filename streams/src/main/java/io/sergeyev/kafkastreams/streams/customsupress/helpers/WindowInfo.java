package io.sergeyev.kafkastreams.streams.customsupress.helpers;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;

@Data
@NoArgsConstructor
public class WindowInfo {
    private Long key;
    private Long startTime;
    private Long endTime;

    public WindowInfo(Windowed<Long> windowed) {
        this.key = windowed.key();
        this.startTime = windowed.window().start();
        this.endTime = windowed.window().end();
    }

    public boolean overlap(Window anotherWindow) {
        return startTime < anotherWindow.end() && anotherWindow.start() < endTime;
    }
}
