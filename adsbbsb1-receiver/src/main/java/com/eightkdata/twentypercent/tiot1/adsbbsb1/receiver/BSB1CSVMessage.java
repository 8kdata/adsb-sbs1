/*
 * Copyright (c) 2016, 8Kdata Technology S.L.
 *
 * Permission to use, copy, modify, and distribute this software and its documentation for any purpose,
 * without fee, and without a written agreement is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL 8Kdata BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES,
 * INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF 8Kdata HAS BEEN
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * 8Kdata SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS,
 * AND 8Kdata HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 */


package com.eightkdata.twentypercent.tiot1.adsbbsb1.receiver;


import javax.annotation.Nonnull;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static com.google.common.base.Preconditions.checkArgument;


public class BSB1CSVMessage {
    private static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");

    private String csvMessage;
    private ZonedDateTime received;

    public BSB1CSVMessage() {}

    public void setCSVMessage(@Nonnull String csvMessage) {
        checkArgument(csvMessage != null && ! csvMessage.isEmpty(), "Null or empty csvMessage");
        this.csvMessage = csvMessage;
        this.received = ZonedDateTime.now(UTC_ZONE_ID);
    }

    public String getCSVMessage() {
        return csvMessage;
    }

    public ZonedDateTime getReceived() {
        return received;
    }

    @Override
    public String toString() {
        return received + " " + csvMessage;
    }
}
