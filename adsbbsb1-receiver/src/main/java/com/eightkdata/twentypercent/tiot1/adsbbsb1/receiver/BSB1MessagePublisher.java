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


import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import javax.annotation.concurrent.NotThreadSafe;

// TODO: this class does not support multiple subscribers and is probably not thread safe, but it should be
// TODO: this publisher does not honor the requested number of items. May publish more than requested

@NotThreadSafe
public class BSB1MessagePublisher implements Publisher<BSB1CSVMessage> {
    private volatile boolean subscribed = false;
    private Subscriber<? super BSB1CSVMessage> subscriber = null;

    @Override
    public void subscribe(Subscriber<? super BSB1CSVMessage> s) {
        s.onSubscribe(new Subscription() {
            @Override
            public void request(long n) {
                if(! subscribed && n > 0) {
                    subscribed = true;
                    subscriber = s;
                }
            }

            @Override
            public void cancel() {
                unsubscribe();
            }
        });
    }

    private void unsubscribe() {
        subscriber.onComplete();
        subscribed = false;
        subscriber = null;
    }

    public void publish(BSB1CSVMessage message) {
        if(subscribed) {
            subscriber.onNext(message);
        }
    }

    public void error(Throwable throwable) {
        if(subscribed) {
            subscriber.onError(throwable);
            unsubscribe();
        }
    }
}
