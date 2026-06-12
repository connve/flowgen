// Joins two upstream batches collected by a `buffer` task into an
// enriched list of orders with a human-readable payment label.
//
// Inputs (event.data.batch): a list of two arrays produced by the fan-in.
// Each array is one of:
//   - orders:        [{ id, order_number, payment_type, amount, ... }, ...]
//   - payment_types: [{ payment_type, label }, ...]
//
// The two arrays may arrive in either order. The script classifies each
// one by checking for a discriminating field on its first row.
//
// Output: a list of enriched order records with a `payment_label` column.

let orders = [];
let payment_types = [];

for batch_part in event.data.batch {
    if batch_part.len() == 0 { continue; }
    let first = batch_part[0];
    if first.keys().contains("order_number") {
        orders = batch_part;
    } else if first.keys().contains("label") {
        payment_types = batch_part;
    }
}

let label_by_payment_type = #{};
for pt in payment_types {
    let key = pt.payment_type;
    if key != () && key != "" {
        label_by_payment_type[key] = pt.label;
    }
}

let output = [];
for o in orders {
    let pt = o.payment_type;
    let label = "";
    if pt != () && pt != "" && label_by_payment_type.contains(pt) {
        label = label_by_payment_type[pt];
    }

    output.push(#{
        id: o.id,
        order_number: o.order_number,
        amount: o.amount,
        payment_type: pt,
        payment_label: label,
    });
}

event.data.batch = output;
event
