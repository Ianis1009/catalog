FROM rust:1.89 AS builder

WORKDIR /app

COPY src/main.rs .

RUN mkdir -p /out

RUN rustc \
    --edition=2021 \
    -C opt-level=3 \
    -C target-feature=+crt-static \
    -o /out/task-scheduler \
    main.rs

FROM gcr.io/distroless/static-debian12

COPY --from=builder /out/task-scheduler /usr/bin/task-scheduler

EXPOSE 8080
EXPOSE 9001

ENTRYPOINT ["/usr/bin/task-scheduler"]